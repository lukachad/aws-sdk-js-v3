import type {
  _Object as S3Object,
  ChecksumAlgorithm,
  CompleteMultipartUploadCommandOutput,
  CreateMultipartUploadCommandInput,
  GetObjectCommandInput,
  GetObjectCommandOutput,
  PutObjectCommandInput,
  PutObjectCommandOutput,
  S3Client,
} from "@aws-sdk/client-s3";
import { HttpHandlerOptions } from "@smithy/types";

import { AddEventListenerOptions, EventHandler, RemoveEventListenerOptions } from "./event-handler-types";

/**
 * Represents a numeric value that can be either number or bigint.
 * Use number for values within safe integer range, bigint for larger values.
 *
 * @public
 */
export type Long = number | bigint;

/**
 * Constructor parameters for the S3 Transfer Manager
 *
 * @param s3ClientInstance - the low level S3 client that will be used to send requests to S3.
 * @param targetPartSizeBytes - the target part size to use in a multipart transfer. Does not apply to downloads if multipartDownloadType is PART
 * @param multipartUploadThresholdBytes - the size threshold, in bytes, for when to use multipart upload.
 * @param checksumValidationEnabled - option to disable checksum validation for download.
 * @param checksumAlgorithm - checksum algorithm to use for upload
 * @param multipartDownloadType - how the SDK should perform multipart download, either RANGE or PART
 * @param progressListener - An array of progress listeners that will be notified of transfer events including initialization, bytes transferred, completion, and failure.
 *
 *
 * @public
 */
export interface S3TransferManagerConfig {
  s3ClientInstance?: S3Client;
  targetPartSizeBytes?: Long;
  multipartUploadThresholdBytes?: Long;
  checksumValidationEnabled?: boolean;
  checksumAlgorithm?: ChecksumAlgorithm;
  multipartDownloadType?: "RANGE" | "PART";
  eventListeners?: TransferEventListeners;
}

/**
 * Uses intersection because requests must include all the required parameters from
 * both PutObjectCommandInput and CreateMultipartUploadCommandInput to support both single object
 * and multipart upload requests.
 *
 * @public
 */
export type UploadRequest = PutObjectCommandInput & CreateMultipartUploadCommandInput;

/**
 * Uses union because the responses can vary from single object upload response to multipart upload
 * response depending on the request.
 *
 * @public
 */
export type UploadResponse = PutObjectCommandOutput | CompleteMultipartUploadCommandOutput;

/**
 * Features the same properties as SDK JS S3 Command GetObjectCommandInput
 *
 * @public
 */
export type DownloadRequest = GetObjectCommandInput;

/**
 * Features the same properties as SDK JS S3 Command GetObjectCommandOutput
 *
 * @public
 */
export type DownloadResponse = GetObjectCommandOutput;

/**
 * Represents a cancellable operation with progress tracking.
 * Provides access to the operation's promise, abort controller, and cancellation status.
 *
 * @public
 */
export interface TransferOperation<status> {
  done: Promise<status>;
  abortController: AbortController;
  isCancelled: boolean;
}

/**
 * Client for efficient transfer of objects to and from Amazon S3.
 * Provides methods to optimize uploading and downloading individual objects
 * as well as entire directories, with support for multipart operations,
 * concurrency control, and request cancellation.
 *
 * @public
 */
export interface S3TransferManager {
  // eslint-disable-next-line @typescript-eslint/no-misused-new
  new (config?: S3TransferManagerConfig): S3TransferManager;

  upload(
    request: UploadRequest,
    options?: HttpHandlerOptions,
    eventListeners?: TransferEventListeners
  ): TransferOperation<UploadResponse>;

  download(
    request: DownloadRequest,
    options?: HttpHandlerOptions,
    eventListeners?: TransferEventListeners
  ): TransferOperation<DownloadResponse>;

  /**
   * Represents an API to upload all files under the given directory to the provided S3 bucket
   *
   * @param options.bucket - The name of the bucket to upload objects to.
   * @param options.source - The source directory to upload.
   * @param options.followSymbolicLinks - Whether to follow symbolic links when traversing the file tree.
   * @param options.recursive - Whether to upload directories recursively.
   * @param options.s3Prefix - The S3 key prefix to use for each object. If not provided, files will be uploaded to the root of the bucket todo()
   * @param options.s3Delimiter - Default "/". The S3 delimiter. A delimiter causes a list operation to roll up all the keys that share a common prefix into a single summary list result.
   * @param options.putObjectRequestCallback - A callback mechanism to allow customers to update individual putObjectRequest that the S3 Transfer Manager generates.
   * @param options.failurePolicy - The failure policy to handle failed requests
   * @param options.progressListener - An array of progress listeners that will be notified of transfer events including initialization, bytes transferred, completion, and failure.
   *
   * @returns The number of objects that have been uploaded and the number of objects that have failed
   */
  uploadAll(options: {
    bucket: string;
    source: string;
    followSymbolicLinks?: boolean;
    recursive?: boolean;
    s3Prefix?: string;
    filter?: (filepath: string) => boolean;
    s3Delimiter?: string;
    putObjectRequestCallback?: (putObjectRequest: PutObjectCommandInput) => Promise<void>;
    failurePolicy?: (error?: unknown) => Promise<void>;
    eventListeners?: TransferEventListeners;
  }): Promise<{
    // defined as long, but should not exceed Max Safe Integer in practical usage.
    objectsUploaded: Long;
    objectsFailed: Long;
  }>;

  /**
   * Represents an API to download all objects under a bucket to the provided local directory.
   *
   * @param options.bucket - The name of the bucket
   * @param options.destination - The destination directory
   * @param options.s3Prefix - Specify the S3 prefix that limits the response to keys that begin with the specified prefix
   * @param options.s3Delimiter - Specify the S3 delimiter.
   * @param options.recursive - Whether to upload directories recursively.
   * @param options.filter - A callback to allow users to filter out unwanted S3 object. It is invoked for each S3 object. An example implementation is a predicate that takes an S3Object and returns a boolean indicating whether this S3Object should be downloaded
   * @param options.getObjectRequestCallback - A callback mechanism to allow customers to update individual getObjectRequest that the S3 Transfer Manager generates.
   * @param options.failurePolicy - The failure policy to handle failed requests
   * @param options.progressListener - An array of progress listeners that will be notified of transfer events including initialization, bytes transferred, completion, and failure.
   *
   * @returns The number of objects that have been uploaded and the number of objects that have failed
   */
  downloadAll(options: {
    bucket: string;
    destination: string;
    s3Prefix?: string;
    s3Delimiter?: string;
    recursive?: boolean;
    filter?: (object?: S3Object) => boolean;
    getObjectRequestCallback?: any;
    failurePolicy?: any;
    eventListeners?: TransferEventListeners;
  }): Promise<{
    objectsDownloaded: Long;
    objectsFailed: Long;
  }>;

  addEventListener(
    type: "transferInitiated",
    callback: EventHandler<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  addEventListener(
    type: "bytesTransferred",
    callback: EventHandler<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  addEventListener(
    type: "transferComplete",
    callback: EventHandler<TransferCompleteEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  addEventListener(
    type: "transferFailed",
    callback: EventHandler<TransferEvent>,
    options?: AddEventListenerOptions | boolean
  ): void;
  addEventListener(type: string, callback: EventHandler | null, options?: AddEventListenerOptions | boolean): void;

  dispatchEvent(event: Event & TransferEvent): boolean;
  dispatchEvent(event: Event & TransferCompleteEvent): boolean;
  dispatchEvent(event: Event): boolean;

  removeEventListener(
    type: "transferInitiated",
    callback: EventHandler<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  removeEventListener(
    type: "bytesTransferred",
    callback: EventHandler<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  removeEventListener(
    type: "transferComplete",
    callback: EventHandler<TransferCompleteEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  removeEventListener(
    type: "transferFailed",
    callback: EventHandler<TransferEvent>,
    options?: RemoveEventListenerOptions | boolean
  ): void;
  removeEventListener(
    type: string,
    callback: EventHandler | null,
    options?: RemoveEventListenerOptions | boolean
  ): void;
}

/**
 * Provides a snapshot of the progress during a single object transfer
 *
 * @public
 */
export interface SingleObjectProgressSnapshot {
  transferredBytes: Long;
  totalBytes?: Long;
  response?: UploadResponse | DownloadResponse;
}

/**
 * Provides a snapshot of the progress during a directory transfer
 *
 * @public
 */
export interface DirectoryProgressSnapshot {
  transferredBytes: Long;
  totalBytes?: Long;
  transferredFiles: Long;
  totalFiles?: Long;
}

/**
 * Progress snapshot for either single object transfers or directory transfers.
 *
 * @public
 */
export type TransferProgressSnapshot = SingleObjectProgressSnapshot | DirectoryProgressSnapshot;

/**
 * Custom event interface
 *
 */
export interface TransferEvent {
  request: UploadRequest | DownloadRequest;
  snapshot: TransferProgressSnapshot;
}

/**
 * Custom event interface for completion
 *
 */
export interface TransferCompleteEvent {
  request: UploadRequest | DownloadRequest;
  response: UploadResponse | DownloadResponse;
  snapshot: TransferProgressSnapshot;
}

/**
 * Interface for tracking transfer progress through event-driven callbacks.
 * Allows monitoring for initialization, bytes transferred, completion, and failure during transfers.
 *
 * @public
 */
export interface TransferEventListeners {
  transferInitiated: EventHandler<TransferEvent>[];
  bytesTransferred: EventHandler<TransferEvent>[];
  transferComplete: EventHandler<TransferCompleteEvent>[];
  transferFailed: EventHandler<TransferEvent>[];
}
