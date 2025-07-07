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
    it("Should throw error for non-sequential ranges", () => {
      const previousRange = "bytes 0-5242879/13631488";
      const invalidRange = "bytes 5242881-10485759/13631488"; // 1 byte off

      expect(() => {
        tm.validateExpectedRanges(previousRange, invalidRange, 2);
      }).toThrow("Expected part 2 to start at 5242880 but got 5242881");
    });
    it("Should throw error for non-sequential ranges", () => {
      const previousRange = "bytes 0-5242879/13631488";
      const invalidRange = "bytes 5242879-10485759/13631488";

      expect(() => {
        tm.validateExpectedRanges(previousRange, invalidRange, 2);
      }).toThrow("Expected part 2 to start at 5242880 but got 5242879");
    });
    it("Should throw error for non-sequential ranges", () => {
      const previousRange = "bytes 0-5242879/13631488";
      const invalidRange = "bytes 0-5242879/13631488";

      expect(() => {
        tm.validateExpectedRanges(previousRange, invalidRange, 2);
      }).toThrow("Expected part 2 to start at 5242880 but got 0");
    });
    it("Should throw error for non-sequential ranges", () => {
      const previousRange = "bytes 0-5242879/13631488";
      const invalidRange = "bytes 0-5242879/13631488";

      expect(() => {
        tm.validateExpectedRanges(previousRange, invalidRange, 2);
      }).toThrow("Expected part 2 to start at 5242880 but got 0");
    });
  });
  // describe("EventTarget functions tests", () => {

  // })
});
