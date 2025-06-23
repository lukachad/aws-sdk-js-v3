import type {
  _Object as S3Object,
  ChecksumAlgorithm,
  CompleteMultipartUploadCommandOutput,
  CreateMultipartUploadCommandInput,
  GetObjectCommandInput,
  GetObjectCommandOutput,
  PutObjectCommandInput,
  PutObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { StreamingBlobPayloadOutputTypes } from "@smithy/types";
import { isReadableStream, sdkStreamMixin } from "@smithy/util-stream";
import { PassThrough, Readable } from "node:stream"; // instead of using node, defer to

import type { AddEventListenerOptions, EventListener, RemoveEventListenerOptions } from "./event-listener-types";
import { joinStreams } from "./join-streams";
import { isNodeStream, isWebStream } from "./stream-guards";
import type {
  DownloadRequest,
  DownloadResponse,
  IS3TransferManager,
  S3TransferManagerConfig,
  TransferCompleteEvent,
  TransferEvent,
  TransferEventListeners,
  TransferOptions,
  UploadRequest,
  UploadResponse,
} from "./types";

export class S3TransferManager implements IS3TransferManager {
  private static MIN_PART_SIZE = 5 * 1024 * 1024; // 5MB
  private static DEFAULT_PART_SIZE = 8 * 1024 * 1024; // 8MB
  private static MIN_UPLOAD_THRESHOLD = 16 * 1024 * 1024; // 16MB

  private readonly s3ClientInstance: S3Client;
  private readonly targetPartSizeBytes: number;
  private readonly multipartUploadThresholdBytes: number;
  private readonly checksumValidationEnabled: boolean;
  private readonly checksumAlgorithm: ChecksumAlgorithm;
  private readonly multipartDownloadType: "PART" | "RANGE";
  private readonly eventListeners: TransferEventListeners;

  public constructor(config: S3TransferManagerConfig = {}) {
    this.s3ClientInstance = config.s3ClientInstance ?? new S3Client({});

    this.targetPartSizeBytes = config.targetPartSizeBytes ?? S3TransferManager.DEFAULT_PART_SIZE;
    this.multipartUploadThresholdBytes = config.multipartUploadThresholdBytes ?? S3TransferManager.MIN_UPLOAD_THRESHOLD;
    this.checksumValidationEnabled = config.checksumValidationEnabled ?? true;
    this.checksumAlgorithm = config.checksumAlgorithm ?? "CRC32";
    this.multipartDownloadType = config.multipartDownloadType ?? "PART";
    this.eventListeners = {
      transferInitiated: config.eventListeners?.transferInitiated ?? [],
      bytesTransferred: config.eventListeners?.bytesTransferred ?? [],
      transferComplete: config.eventListeners?.transferComplete ?? [],
      transferFailed: config.eventListeners?.transferFailed ?? [],
    };

    this.validateConfig();
  }

  public addEventListener(
    type: "transferInitiated",
    callback: EventListener<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  public addEventListener(
    type: "bytesTransferred",
    callback: EventListener<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  public addEventListener(
    type: "transferComplete",
    callback: EventListener<TransferCompleteEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  public addEventListener(
    type: "transferFailed",
    callback: EventListener<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  public addEventListener(
    type: string,
    callback: EventListener | null,
    options?: AddEventListenerOptions | boolean
  ): void;
  public addEventListener(type: unknown, callback: unknown, options?: unknown): void {
    throw new Error("Method not implemented.");
  }

  public dispatchEvent(event: Event & TransferEvent): boolean;
  public dispatchEvent(event: Event & TransferCompleteEvent): boolean;
  public dispatchEvent(event: Event): boolean;
  public dispatchEvent(event: unknown): boolean {
    throw new Error("Method not implemented.");
  }

  public removeEventListener(
    type: "transferInitiated",
    callback: EventListener<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  public removeEventListener(
    type: "bytesTransferred",
    callback: EventListener<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  public removeEventListener(
    type: "transferComplete",
    callback: EventListener<TransferCompleteEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  public removeEventListener(
    type: "transferFailed",
    callback: EventListener<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  public removeEventListener(
    type: string,
    callback: EventListener | null,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  public removeEventListener(type: unknown, callback: unknown, options?: unknown): void {
    throw new Error("Method not implemented.");
  }

  public upload(request: UploadRequest, transferOptions?: TransferOptions): Promise<UploadResponse> {
    throw new Error("Method not implemented.");
  }

  public async download(request: DownloadRequest, transferOptions?: TransferOptions): Promise<DownloadResponse> {
    const metadata = {} as Omit<DownloadResponse, "Body">;
    const streams = [] as StreamingBlobPayloadOutputTypes[];

    const partNumber = request.PartNumber;
    const range = request.Range;

    if (typeof partNumber === "number") {
      // single object download
      const getObject = await this.s3ClientInstance.send(
        new GetObjectCommand({
          ...request,
        }),
        transferOptions
      );

      Object.assign(metadata, getObject, { Body: undefined });
      if (getObject.Body) {
        streams.push(getObject.Body);
      }
    } else if (this.multipartDownloadType === "PART") {
      if (range == null) {
        const initialPart = await this.s3ClientInstance.send(
          new GetObjectCommand({
            ...request,
            PartNumber: 1,
          }),
          transferOptions
        );
        Object.assign(metadata, initialPart, { Body: undefined });
        if (initialPart.Body) {
          streams.push(initialPart.Body);
        }

        if (initialPart.PartsCount! > 1) {
          // MPD entire object
          for (let part = 2; part <= initialPart.PartsCount!; part++) {
            const getObject = await this.s3ClientInstance.send(
              new GetObjectCommand({
                ...request,
                PartNumber: part,
              }),
              transferOptions
            );

            Object.assign(metadata, getObject, { Body: undefined });
            if (getObject.Body) {
              streams.push(getObject.Body);
            }
          }
        } else {
          // single object download
          const getObject = await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
            }),
            transferOptions
          );

          Object.assign(metadata, getObject, { Body: undefined });
          if (getObject.Body) {
            streams.push(getObject.Body);
          }
        }
      } else {
        const getObject = await this.s3ClientInstance.send(
          new GetObjectCommand({
            ...request,
          }),
          transferOptions
        );

        Object.assign(metadata, getObject, { Body: undefined });
        if (getObject.Body) {
          streams.push(getObject.Body);
        }
      }
    } else if (this.multipartDownloadType === "RANGE") {
      // MPD entire object
      if (range == null) {
        console.log("Case 1: range == null");

        const getContentLength = await this.s3ClientInstance.send(
          new GetObjectCommand({
            ...request,
          }),
          transferOptions
        );
        const contentLength = getContentLength.ContentLength ?? 0; // Get total size of the object
        console.log(`Content Length: ${contentLength}`);

        let left = 0;
        let right = 0;
        let remainingLength = contentLength;

        while (remainingLength > S3TransferManager.MIN_PART_SIZE) {
          if (right === 0) {
            // Prevents overlap of bytes
            left = right;
          } else {
            left = right + 1;
            right = right + 1;
          }
          right += S3TransferManager.MIN_PART_SIZE;
          const range = `bytes=${left}-${right}`;

          console.log(`range: ${range}`);

          const getObject = await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
              Range: range,
            }),
            transferOptions
          );

          Object.assign(metadata, getObject, { Body: undefined });
          if (getObject.Body) {
            streams.push(getObject.Body);
          }

          remainingLength = contentLength - right;
          console.log(`Remaining Length: ${remainingLength}`);
        }

        console.log({ remainingLength });

        if (remainingLength > 0) {
          // Download the rest of the object
          const getObject = await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
              Range: `bytes=${right + 1}-${contentLength - 1}`,
            }),
            transferOptions
          );

          console.log(`range: bytes=${right + 1}-${contentLength - 1}`);

          Object.assign(metadata, getObject, { Body: undefined });
          if (getObject.Body) {
            streams.push(getObject.Body);
          }
        }
      } else {
        // MPD user-specified range
        // Treat range as the total content length and split the range based on the MIN_PART_SIZE
        console.log("Case 2: user-specified range");

        const contentLength = parseInt(range.split("-")[1]);
        console.log(`Content Length: ${contentLength}`);
        let left = 0;
        let right = 0;
        let remainingLength = contentLength;

        while (remainingLength > S3TransferManager.MIN_PART_SIZE) {
          if (right === 0) {
            // Prevents overlap of bytes
            left = right;
          } else {
            left = right + 1;
            right = right + 1;
          }
          right += S3TransferManager.MIN_PART_SIZE;

          const range = `bytes=${left}-${right}`;

          console.log(`range: ${range}`);

          const getObject = await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
              Range: range,
            }),
            transferOptions
          );

          Object.assign(metadata, getObject, { Body: undefined });
          if (getObject.Body) {
            streams.push(getObject.Body);
          }

          remainingLength = contentLength - right;
          console.log(`Remaining Length: ${remainingLength}`);
        }

        console.log({ remainingLength });

        if (remainingLength > 0) {
          // Download the rest of the object
          const getObject = await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
              Range: `bytes=${right + 1}-${contentLength - 1}`,
            }),
            transferOptions
          );

          console.log(`range: bytes=${right + 1}-${contentLength - 1}`);
          Object.assign(metadata, getObject, { Body: undefined });
          if (getObject.Body) {
            streams.push(getObject.Body);
          }
        }
      }
    }
    return {
      ...metadata,
      Body: joinStreams(streams),
    };
  }

  public uploadAll(options: {
    bucket: string;
    source: string;
    followSymbolicLinks?: boolean;
    recursive?: boolean;
    s3Prefix?: string;
    filter?: (filepath: string) => boolean;
    s3Delimiter?: string;
    putObjectRequestCallback?: (putObjectRequest: PutObjectCommandInput) => Promise<void>;
    failurePolicy?: (error?: unknown) => Promise<void>;
    transferOptions?: TransferOptions;
  }): Promise<{ objectsUploaded: number; objectsFailed: number }> {
    throw new Error("Method not implemented.");
  }

  public downloadAll(options: {
    bucket: string;
    destination: string;
    s3Prefix?: string;
    s3Delimiter?: string;
    recursive?: boolean;
    filter?: (object?: S3Object) => boolean;
    getObjectRequestCallback?: (getObjectRequest: GetObjectCommandInput) => Promise<void>;
    failurePolicy?: (error?: unknown) => Promise<void>;
    transferOptions?: TransferOptions;
  }): Promise<{ objectsDownloaded: number; objectsFailed: number }> {
    throw new Error("Method not implemented.");
  }

  // protected joinStreams(streams: StreamingBlobPayloadOutputTypes[]): StreamingBlobPayloadOutputTypes {
  //   if (streams.length === 1) {
  //     return streams[0];
  //   }
  //   return sdkStreamMixin(Readable.from(this.iterateStreams(streams)));
  // }

  // protected async *iterateStreams(
  //   streams: StreamingBlobPayloadOutputTypes[]
  // ): AsyncIterable<StreamingBlobPayloadOutputTypes, void, void> {
  //   for (const stream of streams) {
  //     // replace with type check
  //     if (stream instanceof Readable) {
  //       for await (const chunk of stream) {
  //         yield chunk;
  //       }
  //     }
  //   }
  // }

  private validateConfig(): void {
    if (this.targetPartSizeBytes < S3TransferManager.MIN_PART_SIZE) {
      throw new Error(`targetPartSizeBytes must be at least ${S3TransferManager.MIN_PART_SIZE} bytes`);
    }
  }
}
