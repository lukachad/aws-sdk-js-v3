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
import { PassThrough, Readable } from "node:stream";

import type { AddEventListenerOptions, EventListener, RemoveEventListenerOptions } from "./event-listener-types";
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
    const streams = [] as Readable[];

    const partNumber = request.PartNumber;
    const range = request.Range;

    if (typeof partNumber === "number") {
      throw new Error("placeholder: single object download");
    } else if (this.multipartDownloadType === "PART") {
      if (range == null) {
        const getObject = await this.s3ClientInstance.send(
          new GetObjectCommand({
            ...request,
            PartNumber: 1,
          }),
          transferOptions
        );

        if (getObject.PartsCount! > 1) {
          throw new Error("placeholder: multi part download");
        } else {
          throw new Error("placeholder: single object download");
        }
      } else {
        const getObject = await this.s3ClientInstance.send(new GetObjectCommand(request), transferOptions);
        Object.assign(metadata, getObject, { Body: undefined });
        streams.push(getObject.Body as any); // fix this type
      }
    } else if (this.multipartDownloadType == "RANGE") {
      // MPD entire object
      if (range == null) {
        const initialRangeGet = await this.s3ClientInstance.send(
          new GetObjectCommand({
            ...request,
            Range: `bytes=0-${S3TransferManager.MIN_PART_SIZE}`,
          }),
          transferOptions
        );
        const contentLength = initialRangeGet.ContentLength ?? 0; // Get total size of the object
        console.log(`Content Length: ${contentLength}`);

        // Perform Multipart Download
        // Check ContentLength from the response. If the contentLength is larger than the MIN_PART_SIZE, continue to send more parts until the last part is finished.
        if (contentLength > 0) {
          let left = 0;
          let right = S3TransferManager.MIN_PART_SIZE;
          let remainingLength = contentLength;

          while (remainingLength > S3TransferManager.MIN_PART_SIZE) {
            const range = `bytes=${left}-${right}`;

            console.log(`Remaining Length: ${remainingLength}`);
            console.log(range);

            await this.s3ClientInstance.send(
              new GetObjectCommand({
                ...request,
                Range: range,
              }),
              transferOptions
            );
            remainingLength = contentLength - right;

            left = right + 1;
            right = right + S3TransferManager.MIN_PART_SIZE;
          }

          // Download the rest of the object
          console.log({ remainingLength });

          return await this.s3ClientInstance.send(
            new GetObjectCommand({
              ...request,
              Range: `bytes=${right + 1}-${contentLength}`,
            }),
            transferOptions
          );
        }
      } else {
        // MPD user-specified range
        // Treat range as the total content length and split the range based on the MIN_PART_SIZE
      }
    }

    const userStream = new PassThrough();
    streams.forEach((stream) => {
      // connect all the streams together without buffering
      // their data.
      stream.pipe(userStream);
    });

    return {
      ...metadata,
      Body: userStream as any,
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

  private validateConfig(): void {
    if (this.targetPartSizeBytes < S3TransferManager.MIN_PART_SIZE) {
      throw new Error(`targetPartSizeBytes must be at least ${S3TransferManager.MIN_PART_SIZE} bytes`);
    }
  }
}
