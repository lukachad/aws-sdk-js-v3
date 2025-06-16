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
  private readonly targetPartSizeBytes: number = S3TransferManager.DEFAULT_PART_SIZE;
  private readonly multipartUploadThresholdBytes: number = S3TransferManager.MIN_UPLOAD_THRESHOLD;
  private readonly checksumValidationEnabled: boolean = true;
  private readonly checksumAlgorithm: string = "CRC32";
  private readonly multipartDownloadType: string = "PART";
  private readonly eventListeners: TransferEventListeners;

  public constructor(config: S3TransferManagerConfig = {}) {
    this.s3ClientInstance = config.s3ClientInstance ?? new S3Client({});
    this.targetPartSizeBytes = config.targetPartSizeBytes ?? this.targetPartSizeBytes;
    this.multipartUploadThresholdBytes = config.multipartUploadThresholdBytes ?? this.multipartUploadThresholdBytes;
    this.checksumValidationEnabled = config.checksumValidationEnabled ?? this.checksumValidationEnabled;
    this.checksumAlgorithm = config.checksumAlgorithm ?? this.checksumAlgorithm;
    this.multipartDownloadType = config.multipartDownloadType ?? this.multipartDownloadType;
    this.eventListeners = {
      transferInitiated: config.eventListeners?.transferInitiated ?? [],
      bytesTransferred: config.eventListeners?.bytesTransferred ?? [],
      transferComplete: config.eventListeners?.transferComplete ?? [],
      transferFailed: config.eventListeners?.transferFailed ?? [],
    };

    this.__validateConfig();
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
    let isSingleObjectRequest = true;
    const partNumber = request.PartNumber?.valueOf();
    const range = request.Range?.valueOf();

    if (partNumber != null) {
      isSingleObjectRequest = true;
    }

    if (this.multipartDownloadType == "PART") {
      if (range == null) {
        const newRequest = { ...request };
        newRequest.PartNumber = 1;
        const response = await this.s3ClientInstance.send(new GetObjectCommand(newRequest), transferOptions);
        const partsCount = response.PartsCount?.valueOf() ?? 0;
        if (partsCount > 1) {
          // Use Multipart Download
          isSingleObjectRequest = false;
        }
      } else {
        isSingleObjectRequest = true;
      }
    } else {
      // if (this.multipartDownloadType == 'RANGE')
      // Use Multipart Download
      isSingleObjectRequest = false;

      if (range == null) {
        const newRequest = { ...request };
        newRequest.Range = `bytes=0-${S3TransferManager.MIN_PART_SIZE}`;
        const response = await this.s3ClientInstance.send(new GetObjectCommand(newRequest), transferOptions);
        const contentLength = response.ContentLength?.valueOf() ?? 0;
        // Perform Multipart Download
        // Check ContentLength from the response. If the contentLength is larger than the MIN_PART_SIZE, continue to send more parts until the last part is finished.
      } else {
        // Perform Multipart Download
        // Treat range as the total content length and split the range based on the MIN_PART_SIZE
      }
    }

    if (isSingleObjectRequest) {
      console.log("use single object download");
      return this.__singleObjectDownload(request, transferOptions);
    } else {
      console.log("use multipart download");
      return this.__singleObjectDownload(request, transferOptions);
    }
  }

  private async __singleObjectDownload(
    request: DownloadRequest,
    transferOptions?: TransferOptions
  ): Promise<GetObjectCommandOutput> {
    const getObjectOutput = await this.s3ClientInstance.send(new GetObjectCommand(request), transferOptions);
    return getObjectOutput;
  }

  // Is range parser required? Example range input: "bytes=0-1023"
  private __parseRange() {
    throw new Error("Method not implemented.");
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

  private __validateConfig(): void {
    if (this.targetPartSizeBytes < S3TransferManager.MIN_PART_SIZE) {
      throw new Error(`targetPartSizeBytes must be at least ${S3TransferManager.MIN_PART_SIZE} bytes`);
    }
  }
}
