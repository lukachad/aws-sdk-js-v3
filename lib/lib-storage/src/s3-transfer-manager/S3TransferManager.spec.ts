import { S3, S3Client } from "@aws-sdk/client-s3";
import { TransferCompleteEvent, TransferEvent } from "@aws-sdk/lib-storage/dist-types/s3-transfer-manager/types";
import { beforeAll, describe, expect, test as it, vi } from "vitest";

import { S3TransferManager } from "./S3TransferManager";

/**
 * Unit Tests:
 * - addEventListener()
 * - dispatchEvent()
 * - removeEventListener()
 * - TM Constructor
 * - *iterateListeners()
 * - validateExpectedRanges()
 */

describe("S3TransferManager Unit Tests", () => {
  let client: S3;
  let Bucket: string;
  let region: string;

  beforeAll(async () => {
    region = "us-west-1";
    Bucket = "lukachad-us-west-2";

    client = new S3({
      region,
      responseChecksumValidation: "WHEN_REQUIRED",
    });
  });
  describe("S3TransferManager Constructor", () => {
    it("Should create an instance of S3TransferManager with defaults given no parameters", () => {
      const defaultS3Client = new S3Client({
        requestChecksumCalculation: "WHEN_SUPPORTED",
        responseChecksumValidation: "WHEN_SUPPORTED",
      });
      const tm = new S3TransferManager() as any;

      expect(tm.s3ClientInstance).toBeInstanceOf(S3Client);
      expect(tm.targetPartSizeBytes).toBe(8 * 1024 * 1024);
      expect(tm.multipartUploadThresholdBytes).toBe(16 * 1024 * 1024);
      expect(tm.checksumValidationEnabled).toBe(true);
      expect(tm.checksumAlgorithm).toBe("CRC32");
      expect(tm.multipartDownloadType).toBe("PART");
      expect(tm.eventListeners).toEqual({
        transferInitiated: [],
        bytesTransferred: [],
        transferComplete: [],
        transferFailed: [],
      });
    });

    it("Should create an instance of S3TransferManager with all given parameters", () => {
      const eventListeners = {
        transferInitiated: [() => console.log("transferInitiated")],
        bytesTransferred: [() => console.log("bytesTransferred")],
        transferComplete: [() => console.log("transferComplete")],
        transferFailed: [() => console.log("transferFailed")],
      };
      const tm = new S3TransferManager({
        s3ClientInstance: client,
        targetPartSizeBytes: 8 * 1024 * 1024,
        checksumValidationEnabled: true,
        checksumAlgorithm: "CRC32",
        multipartDownloadType: "RANGE",
        eventListeners: eventListeners,
      }) as any;

      expect(tm.s3ClientInstance).toBe(client);
      expect(tm.targetPartSizeBytes).toBe(8 * 1024 * 1024);
      expect(tm.checksumValidationEnabled).toBe(true);
      expect(tm.checksumAlgorithm).toBe("CRC32");
      expect(tm.multipartDownloadType).toBe("RANGE");
      expect(tm.eventListeners).toEqual(eventListeners);
    });

    it("Should throw an error given targetPartSizeBytes smaller than minimum", () => {
      expect(() => {
        new S3TransferManager({
          targetPartSizeBytes: 2 * 1024 * 1024,
        });
      }).toThrow(`targetPartSizeBytes must be at least ${5 * 1024 * 1024} bytes`);
    });
  });

  describe("EventListener functions", () => {
    let tm: S3TransferManager;

    function initiated(event: TransferEvent) {
      return {
        request: event.request,
        snapshot: event.snapshot,
      };
    }
    function transferring(event: TransferEvent) {
      return {
        request: event.request,
        snapshot: event.snapshot,
      };
    }
    function completed(event: TransferCompleteEvent) {
      return {
        request: event.request,
        snapshot: event.snapshot,
        response: event.response,
      };
    }
    function failed(event: TransferEvent) {
      return {
        request: event.request,
        snapshot: event.snapshot,
      };
    }

    beforeAll(async () => {
      tm = new S3TransferManager({
        s3ClientInstance: client,
      });
    });

    describe("addEventListener", () => {
      it("Should add a new listener for each callback", () => {
        tm.addEventListener("transferInitiated", initiated);
        tm.addEventListener("bytesTransferred", transferring);
        tm.addEventListener("transferComplete", completed);
        tm.addEventListener("transferFailed", failed);

        expect((tm as any).eventListeners).toEqual({
          transferInitiated: [initiated],
          bytesTransferred: [transferring],
          transferComplete: [completed],
          transferFailed: [failed],
        });
      });

      it("Should append new listeners for a callback");
    });

    describe.skip("dispatchEvent"), () => {};

    describe.skip("removeEventListener"), () => {};
  });

  describe("validateExpectedRanges()", () => {
    let tm: any;
    beforeAll(async () => {
      tm = new S3TransferManager() as any;
    }, 120_000);

    it("Should pass correct sequential ranges without throwing an error", () => {
      const ranges = [
        "bytes 0-5242879/13631488",
        "bytes 5242880-10485759/13631488",
        "bytes 10485760-13631487/13631488",
      ];

      for (let i = 1; i < ranges.length; i++) {
        expect(() => {
          tm.validateExpectedRanges(ranges[i - 1], ranges[i], i + 1);
        }).not.toThrow();
      }
    });

    it("Should throw error for incomplete download", () => {
      const ranges = [
        "bytes 0-5242879/13631488",
        "bytes 5242880-10485759/13631488",
        "bytes 10485760-13631480/13631488", // 8 bytes short
      ];

      expect(() => {
        tm.validateExpectedRanges(ranges[1], ranges[2], 3);
      }).toThrow(
        "Range validation failed: Final part did not cover total range of 13631488. Expected range of bytes 10485760-314572"
      );
    });

    it.each([
      ["bytes 5242881-10485759/13631488", "Expected part 2 to start at 5242880 but got 5242881"], // 1 byte off
      ["bytes 5242879-10485759/13631488", "Expected part 2 to start at 5242880 but got 5242879"], // overlap
      ["bytes 0-5242879/13631488", "Expected part 2 to start at 5242880 but got 0"], // duplicate
    ])("Should throw error for non-sequential range: %s", (invalidRange, expectedError) => {
      expect(() => {
        tm.validateExpectedRanges("bytes 0-5242879/13631488", invalidRange, 2);
      }).toThrow(expectedError);
    });
  });
});
