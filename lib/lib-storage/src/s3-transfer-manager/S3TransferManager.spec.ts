import { S3, S3Client } from "@aws-sdk/client-s3";
import { TransferCompleteEvent, TransferEvent } from "@aws-sdk/lib-storage/dist-types/s3-transfer-manager/types";
import { beforeAll, beforeEach, describe, expect, test as it, vi } from "vitest";

import { S3TransferManager } from "./S3TransferManager";

/**
 * Unit Tests:
 * - addEventListener()
 * - dispatchEvent()
 * - removeEventListener()
 * - TM Constructor
 * - *iterateListeners()
 * - joinStreams()
 * - iterateStreams()
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

    beforeEach(async () => {
      tm = new S3TransferManager({
        s3ClientInstance: client,
      });
    });

    describe("addEventListener()", () => {
      it("Should register callbacks for all supported event types", () => {
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

      it("Should handle registering the same listener multiple times", () => {
        const callback1 = vi.fn();
        tm.addEventListener("transferInitiated", callback1);
        tm.addEventListener("transferInitiated", callback1);

        expect((tm as any).eventListeners.transferInitiated).toEqual([callback1, callback1]);
      });

      it("Should handle different callbacks for the same event type", () => {
        const callback1 = vi.fn();
        const callback2 = vi.fn();

        tm.addEventListener("bytesTransferred", callback1);
        tm.addEventListener("bytesTransferred", callback2);

        expect((tm as any).eventListeners.bytesTransferred).toEqual([callback1, callback2]);
      });

      it("Should handle object-style callbacks", () => {
        const objectCallback = {
          handleEvent: vi.fn(),
        };
        tm.addEventListener("transferInitiated", objectCallback as any);

        expect((tm as any).eventListeners.transferInitiated).toEqual([objectCallback]);
      });

      it("Should handle a mix of object-style callbacks and function for the same event", () => {
        const callback = vi.fn();
        const objectCallback = {
          handleEvent: vi.fn(),
        };
        tm.addEventListener("transferInitiated", objectCallback as any);
        tm.addEventListener("transferInitiated", callback);

        expect((tm as any).eventListeners.transferInitiated).toEqual([objectCallback, callback]);
      });

      it("Should throw an error for an invalid event type", () => {
        expect(() => {
          (tm as any).addEventListener("invalidEvent", initiated);
        }).toThrow("Unknown event type: invalidEvent");
      });

      it("Should handle options.once correctly, running the listener at most once.", () => {
        const mockCallback = vi.fn();
        tm.addEventListener("transferInitiated", mockCallback, { once: true });

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);
        tm.dispatchEvent(event);

        expect(mockCallback).toHaveBeenCalledTimes(1);
      });

      it("Should not add listener if included AbortSignal is aborted", () => {
        const controller = new AbortController();
        const callback = vi.fn();
        controller.abort();
        tm.addEventListener("transferInitiated", callback, { signal: controller.signal });
        expect((tm as any).eventListeners.transferInitiated).toEqual([]);
      });

      it("Should remove listener after included AbortSignal was aborted", () => {
        const controller = new AbortController();
        const callback = vi.fn();
        tm.addEventListener("transferInitiated", callback, { signal: controller.signal });

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });
        tm.dispatchEvent(event);

        expect(callback).toHaveBeenCalledTimes(1);
        expect((tm as any).eventListeners.transferInitiated).toEqual([callback]);

        controller.abort();
        expect((tm as any).eventListeners.transferInitiated).toEqual([]);
      });

      it("Should clean up abort listeners and store cleanup functions in WeakMap", () => {
        const controller = new AbortController();
        const callback = vi.fn();

        tm.addEventListener("transferInitiated", callback, { signal: controller.signal });

        expect((tm as any).eventListeners.transferInitiated).toEqual([callback]);
        expect((tm as any).abortCleanupFunctions.has(controller.signal)).toBe(true);

        const cleanupFn = (tm as any).abortCleanupFunctions.get(controller.signal);
        cleanupFn();
        (tm as any).abortCleanupFunctions.delete(controller.signal);

        expect((tm as any).abortCleanupFunctions.has(controller.signal)).toBe(false);
        controller.abort();
        expect((tm as any).eventListeners.transferInitiated).toEqual([callback]);
      });

      it("Should handle boolean options parameter", () => {
        tm.addEventListener("transferInitiated", initiated, true);
        expect((tm as any).eventListeners.transferInitiated).toContain(initiated);
      });

      it("Should handle null callback", () => {
        expect(() => {
          (tm as any).addEventListener("transferInitiated", null);
        }).not.toThrow();
      });

      it("Should handle object-style callback with handleEvent", () => {
        const objectCallback = { handleEvent: vi.fn() };
        tm.addEventListener("transferInitiated", objectCallback as any);
        expect((tm as any).eventListeners.transferInitiated).toContain(objectCallback);
      });
    });

    describe("dispatchEvent()", () => {
      it("Should dispatch an event", () => {
        const mockCallback = vi.fn();
        tm.addEventListener("bytesTransferred", mockCallback);

        const event = Object.assign(new Event("bytesTransferred"), {
          request: {},
          snapshot: {},
        });

        const result = tm.dispatchEvent(event);

        expect(mockCallback).toHaveBeenCalledTimes(1);
        expect(mockCallback).toHaveBeenCalledWith(event);
        expect(result).toBe(true);
      });

      it("Should dispatch an event with request, snapshot, and response information", () => {
        const mockCompleted = vi.fn().mockImplementation(completed);
        tm.addEventListener("transferComplete", mockCompleted);

        const event = Object.assign(new Event("transferComplete"), {
          request: { bucket: "test" },
          snapshot: { bytes: 100 },
          response: { status: "success" },
        });

        tm.dispatchEvent(event);

        expect(mockCompleted).toHaveBeenCalledWith(event);
        expect(mockCompleted).toHaveReturnedWith({
          request: { bucket: "test" },
          snapshot: { bytes: 100 },
          response: { status: "success" },
        });
      });

      it("Should call multiple listeners for the same event type", () => {
        const mockCallback = vi.fn();
        tm.addEventListener("transferInitiated", mockCallback);
        tm.addEventListener("transferInitiated", mockCallback);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        const result = tm.dispatchEvent(event);

        expect(mockCallback).toHaveBeenCalledTimes(2);
        expect(mockCallback).toHaveBeenCalledWith(event);
        expect(result).toBe(true);
      });

      it("Should call listeners in the order they were added", () => {
        const callOrder: number[] = [];
        const mockCallback1 = vi.fn(() => callOrder.push(1));
        const mockCallback2 = vi.fn(() => callOrder.push(2));
        const mockCallback3 = vi.fn(() => callOrder.push(3));

        tm.addEventListener("transferInitiated", mockCallback1);
        tm.addEventListener("transferInitiated", mockCallback2);
        tm.addEventListener("transferInitiated", mockCallback3);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(callOrder).toEqual([1, 2, 3]);
      });

      it("Should handle object-style callbacks with handleEvent method", () => {
        const mockCallback = vi.fn();
        const objectCallback = {
          handleEvent: mockCallback,
        };
        tm.addEventListener("transferInitiated", objectCallback as any);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(mockCallback).toHaveBeenCalledTimes(1);
        expect(mockCallback).toHaveBeenCalledWith(event);
      });

      it("Should handle events with no registered listeners", () => {
        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });
        const result = tm.dispatchEvent(event);

        expect(result).toBe(true);
      });

      it("Should handle unknown event types", () => {
        const event = Object.assign(new Event("unknownEvent"), {
          request: {},
          snapshot: {},
        });

        const results = tm.dispatchEvent(event);
        expect(results).toBe(true);
      });

      it("Should handle a mix of object-style callbacks and functions", () => {
        const callback = vi.fn();
        const objectCallback = {
          handleEvent: vi.fn(),
        };
        tm.addEventListener("transferInitiated", objectCallback as any);
        tm.addEventListener("transferInitiated", callback);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(objectCallback.handleEvent).toHaveBeenCalledTimes(1);
        expect(objectCallback.handleEvent).toHaveBeenCalledWith(event);
        expect(callback).toHaveBeenCalledTimes(1);
        expect(callback).toHaveBeenCalledWith(event);
      });
    });

    describe("removeEventListener()", () => {
      it("Should remove only the specified listener, leaving other intact", () => {
        const callback1 = vi.fn();
        const callback2 = vi.fn();
        tm.addEventListener("transferInitiated", callback1);
        tm.addEventListener("transferInitiated", callback2);

        tm.removeEventListener("transferInitiated", callback1);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(callback1).not.toHaveBeenCalled();
        expect(callback2).toHaveBeenCalledTimes(1);
      });

      it("Should remove object-style callback with handleEvent", () => {
        const objectCallback = { handleEvent: vi.fn() };
        tm.addEventListener("transferInitiated", objectCallback as any);
        tm.removeEventListener("transferInitiated", objectCallback as any);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);
        expect(objectCallback.handleEvent).not.toHaveBeenCalled();
      });

      it("Should remove all instance of the same callback", () => {
        const callback = vi.fn();
        tm.addEventListener("transferInitiated", callback);
        tm.addEventListener("transferInitiated", callback);

        tm.removeEventListener("transferInitiated", callback);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(callback).not.toHaveBeenCalled();
      });

      it("Should handle removing non-existing listener gracefully", () => {
        const callback = vi.fn();
        expect(() => {
          tm.removeEventListener("transferInitiated", callback);
        }).not.toThrow();
      });

      it("Should handle removing from an event type with no listeners gracefully", () => {
        const callback = vi.fn();
        tm.removeEventListener("transferInitiated", callback);

        const event = Object.assign(new Event("transferInitiated"), {
          request: {},
          snapshot: {},
        });

        tm.dispatchEvent(event);

        expect(callback).not.toHaveBeenCalled();
      });

      it("Should handle null callback parameter", () => {
        expect(() => {
          tm.removeEventListener("transferInitiated", null as any);
        }).not.toThrow();
      });
    });

    describe("iterateListeners()", () => {
      it("Should iterate over all listeners given a TransferManager's object of event listeners", () => {
        const callback1 = vi.fn();
        const callback2 = vi.fn();
        const callback3 = vi.fn();

        const eventListeners = {
          transferInitiated: [callback1],
          bytesTransferred: [callback2, callback3],
          transferComplete: [],
          transferFailed: [],
        };

        const results = Array.from((tm as any).iterateListeners(eventListeners)) as any[];

        expect(results).toHaveLength(3);
        expect(results[0][0]).toEqual({ eventType: "transferInitiated", callback: callback1 });
        expect(results[1][0]).toEqual({ eventType: "bytesTransferred", callback: callback2 });
        expect(results[2][0]).toEqual({ eventType: "bytesTransferred", callback: callback3 });
      });

      it("Should handle empty event listeners object", () => {
        const eventListeners = {
          transferInitiated: [],
          bytesTransferred: [],
          transferComplete: [],
          transferFailed: [],
        };

        const results = Array.from((tm as any).iterateListeners(eventListeners)) as any[];

        expect(results).toHaveLength(0);
      });

      it("Should iterate over a mix of functions and objects with handleEvent callback types.", () => {
        const callback1 = vi.fn();
        const callback2 = vi.fn();
        const objectCallback = {
          handleEvent: vi.fn(),
        };

        const eventListeners = {
          transferInitiated: [callback1],
          bytesTransferred: [],
          transferComplete: [],
          transferFailed: [callback2, objectCallback],
        };

        const results = Array.from((tm as any).iterateListeners(eventListeners)) as any[];

        expect(results).toHaveLength(3);
        expect(results[0][0]).toEqual({ eventType: "transferInitiated", callback: callback1 });
        expect(results[1][0]).toEqual({ eventType: "transferFailed", callback: callback2 });
        expect(results[2][0]).toEqual({ eventType: "transferFailed", callback: objectCallback });
      });

      it("Should handle event lisetners with duplicate callbacks in the same event type", () => {
        const callback = vi.fn();

        const eventListeners = {
          transferInitiated: [callback, callback],
          bytesTransferred: [],
          transferComplete: [callback, callback],
          transferFailed: [],
        };

        const results = Array.from((tm as any).iterateListeners(eventListeners)) as any[];

        expect(results).toHaveLength(4);
        for (let i = 0; i < results.length; i++) {
          expect(results[i][0]).toEqual({ eventType: results[i][0].eventType, callback });
        }
      });

      it("Should return empty iterator when no callbacks are present", () => {
        const eventListeners = {};

        const results = Array.from((tm as any).iterateListeners(eventListeners)) as any[];

        expect(results).toHaveLength(0);
      });
    });

    describe.skip("joinStreams()", () => {});

    describe.skip("iterateStreams()", () => {});
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
