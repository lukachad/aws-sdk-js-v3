import { describe, expect, test as it } from "vitest";

import { S3TransferManager } from "./S3TransferManager";

describe("S3TransferManager Unit Tests", () => {
  describe("validateExpectedRanges()", () => {
    it("Should pass correct ranges without throwing an error", async () => {
      const tm = new S3TransferManager();

      // const ranges = [{""}];
    });
  });
});
