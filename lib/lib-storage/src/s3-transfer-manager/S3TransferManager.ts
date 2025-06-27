import type {
  _Object as S3Object,
  ChecksumAlgorithm,
  GetObjectCommandInput,
  PutObjectCommandInput,
} from "@aws-sdk/client-s3";
import { GetObjectCommand, HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getChecksum } from "@aws-sdk/middleware-flexible-checksums/dist-types/getChecksum";
import { ChecksumConstructor, StreamingBlobPayloadOutputTypes } from "@smithy/types";
import { Checksum } from "@smithy/types";

import type { AddEventListenerOptions, EventListener, RemoveEventListenerOptions } from "./event-listener-types";
import { joinStreams } from "./join-streams";
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
    this.checksumValidationEnabled = config.checksumValidationEnabled ?? true;

    const checksumMode = this.checksumValidationEnabled ? "WHEN_SUPPORTED" : "WHEN_REQUIRED";

    this.s3ClientInstance =
      config.s3ClientInstance ??
      new S3Client({
        requestChecksumCalculation: checksumMode,
        responseChecksumValidation: checksumMode,
      });

    this.targetPartSizeBytes = config.targetPartSizeBytes ?? S3TransferManager.DEFAULT_PART_SIZE;
    this.multipartUploadThresholdBytes = config.multipartUploadThresholdBytes ?? S3TransferManager.MIN_UPLOAD_THRESHOLD;

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
  public dispatchEvent(event: any): boolean {
    const eventType = event.type;
    const listeners = this.eventListeners[eventType as keyof TransferEventListeners];

    if (listeners) {
      for (const callback of listeners) {
        if (typeof callback === "function") {
          callback(event);
        } else {
          callback.handleEvent?.(event);
        }
      }
    }
    return true;
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
    const requests = [] as GetObjectCommandInput[];

    const partNumber = request.PartNumber;
    const range = request.Range;
    let totalSize = 0;

    if (typeof partNumber === "number") {
      // single object download
      const getObjectRequest = {
        ...request,
      };
      const getObject = await this.s3ClientInstance.send(new GetObjectCommand(getObjectRequest), transferOptions);

      this.dispatchEvent(
        Object.assign(new Event("transferInitiated"), {
          request,
          snapshot: {
            transferredBytes: 0,
            totalBytes: getObject.ContentLength,
          },
        })
      );

      if (getObject.Body) {
        streams.push(getObject.Body);
        requests.push(getObjectRequest);
      }
      this.assignMetadata(metadata, getObject);
    } else if (this.multipartDownloadType === "PART") {
      const headObject = await this.s3ClientInstance.send(
        new HeadObjectCommand({
          Bucket: request.Bucket,
          Key: request.Key,
        }),
        transferOptions
      );

      totalSize = headObject.ContentLength ?? 0;

      if (range == null) {
        const initialPartRequest = {
          ...request,
          PartNumber: 1,
        };
        const initialPart = await this.s3ClientInstance.send(new GetObjectCommand(initialPartRequest), transferOptions);

        this.dispatchTransferInitiatedEvent(request, totalSize);
        console.log(`Initial Part Content Length: ${initialPart.ContentLength}`);

        if (initialPart.Body) {
          streams.push(initialPart.Body);
          requests.push(initialPartRequest);
        }
        this.assignMetadata(metadata, initialPart);

        if (initialPart.PartsCount! > 1) {
          for (let part = 2; part <= initialPart.PartsCount!; part++) {
            const getObjectRequest = {
              ...request,
              PartNumber: part,
            };
            const getObject = await this.s3ClientInstance.send(new GetObjectCommand(getObjectRequest), transferOptions);

            if (getObject.Body) {
              streams.push(getObject.Body);
              requests.push(getObjectRequest);
            }
            this.assignMetadata(metadata, getObject);
          }
        }
      } else {
        const getObjectRequest = {
          ...request,
        };
        const getObject = await this.s3ClientInstance.send(new GetObjectCommand(getObjectRequest), transferOptions);

        this.dispatchTransferInitiatedEvent(request, getObject.ContentLength);

        if (getObject.Body) {
          streams.push(getObject.Body);
          requests.push(getObjectRequest);
        }
        this.assignMetadata(metadata, getObject);
      }
    } else if (this.multipartDownloadType === "RANGE") {
      const headObject = await this.s3ClientInstance.send(
        new HeadObjectCommand({
          Bucket: request.Bucket,
          Key: request.Key,
        }),
        transferOptions
      );

      totalSize = headObject.ContentLength ?? 0;

      let left = 0;
      let right = S3TransferManager.MIN_PART_SIZE;
      let maxRange = Infinity;

      if (range != null) {
        const [userRangeLeft, userRangeRight] = range.replace("bytes=", "").split("-").map(Number);

        maxRange = userRangeRight;
        left = userRangeLeft;
        right = Math.min(userRangeRight, left + S3TransferManager.MIN_PART_SIZE);
      }

      let remainingLength = 1;
      let transferInitiatedEventDispatched = false;

      while (remainingLength > 0) {
        const range = `bytes=${left}-${right}`;
        const getObjectRequest = {
          ...request,
          Range: range,
        };
        const getObject = await this.s3ClientInstance.send(new GetObjectCommand(getObjectRequest), transferOptions);

        if (!transferInitiatedEventDispatched) {
          this.dispatchTransferInitiatedEvent(request, totalSize);
          transferInitiatedEventDispatched = true;
        }

        if (getObject.Body) {
          streams.push(getObject.Body);
          requests.push(getObjectRequest);

          // todo: acquire lock on webstreams in the same
          // todo: synchronous frame as they are opened or else
          // todo: the connection might be closed too early.
          if (typeof (getObject.Body as ReadableStream).getReader === "function") {
            const reader = (getObject.Body as any).getReader();
            (getObject.Body as any).getReader = function () {
              return reader;
            };
          }
        }
        this.assignMetadata(metadata, getObject);

        left = right + 1;
        right = Math.min(left + S3TransferManager.MIN_PART_SIZE, maxRange);

        remainingLength = Math.min(
          right - left,
          Math.max(0, (getObject.ContentLength ?? 0) - S3TransferManager.MIN_PART_SIZE)
        );
      }
    }

    // Create new array of same length of streams array, the requests of each stream
    const responseBody = joinStreams(streams, {
      onBytes: (byteLength: number, index) => {
        this.dispatchEvent(
          Object.assign(new Event("bytesTransferred"), {
            requests: requests[index],
            snapshot: {
              transferredBytes: byteLength,
              totalBytes: totalSize,
            },
          })
        );
      },
      onCompletion: (byteLength: number, index) => {
        this.dispatchEvent(
          Object.assign(new Event("transferComplete"), {
            requests: requests[index],
            response: {
              ...metadata,
              Body: responseBody,
            },
            snapshot: {
              transferredBytes: byteLength,
              totalBytes: totalSize,
            },
          })
        );
      },
      onFailure: (error: unknown, index) => {
        this.dispatchEvent(
          Object.assign(new Event("transferFailed"), {
            requests: requests[index],
            snapshot: {
              transferredBytes: 0,
              totalBytes: totalSize,
            },
          })
        );
      },
    });

    const response = {
      ...metadata,
      Body: responseBody,
    };

    return response;
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

  private assignMetadata(container: any, response: any) {
    for (const key in response) {
      if (key === "Body") {
        continue;
      }
      container[key] = response[key];
    }
  }

  private validateConfig(): void {
    if (this.targetPartSizeBytes < S3TransferManager.MIN_PART_SIZE) {
      throw new Error(`targetPartSizeBytes must be at least ${S3TransferManager.MIN_PART_SIZE} bytes`);
    }

    if (
      this.checksumAlgorithm != "CRC32" &&
      this.checksumAlgorithm != "CRC32C" &&
      this.checksumAlgorithm != "CRC64NVME" &&
      this.checksumAlgorithm != "SHA1" &&
      this.checksumAlgorithm != "SHA256"
    ) {
      throw new Error(
        `Invalid checksumAlgorithm. Must be one of the following: CRC32, CRC32C, CRC64NVME, SHA1, SHA256`
      );
    }
  }

  private dispatchTransferInitiatedEvent(request: DownloadRequest | UploadRequest, totalSize?: number): boolean {
    this.dispatchEvent(
      Object.assign(new Event("transferInitiated"), {
        request,
        snapshot: {
          transferredBytes: 0,
          totalBytes: totalSize,
        },
      })
    );
    return true;
  }
}
