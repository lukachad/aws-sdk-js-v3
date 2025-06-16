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
import { S3Client } from "@aws-sdk/client-s3";
import { Config } from "prettier";
import { options } from "yargs";

import { EventListener } from "./event-listener-types";
import {
  S3TransferManager,
  S3TransferManagerConfig,
  TransferCompleteEvent,
  TransferEvent,
  TransferEventListeners,
} from "./types";

class TransferManager {
  /**
   * @internal
   */
  private static MIN_PART_SIZE = 5 * 1024 * 1024; // 5MB
  private static DEFAULT_PART_SIZE = 8 * 1024 * 1024; // 8MB
  private static MIN_UPLOAD_THRESHOLD = 16 * 1024 * 1024; // 16MB

  // Defaults:
  private readonly s3ClientInstance: S3Client;
  private readonly targetPartSizeBytes: number = TransferManager.DEFAULT_PART_SIZE;
  private readonly multipartUploadThresholdBytes: number = TransferManager.MIN_UPLOAD_THRESHOLD;
  private readonly checksumValidationEnabled: boolean = true;
  private readonly checksumAlgorithm: string = "CRC32";
  private readonly multipartDownloadType: string = "PART";
  private readonly eventListeners: TransferEventListeners;

  constructor(config: S3TransferManagerConfig) {
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

  private __validateConfig(): void {
    if (this.targetPartSizeBytes < TransferManager.MIN_PART_SIZE) {
      throw new Error(`targetPartSizeBytes must be at least ${TransferManager.MIN_PART_SIZE} bytes`);
    }
  }
}
