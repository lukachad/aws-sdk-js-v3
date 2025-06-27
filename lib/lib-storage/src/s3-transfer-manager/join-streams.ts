import { DownloadRequest } from "@aws-sdk/lib-lib-storage/dist-types/s3-transfer-manager/types";
import { StreamingBlobPayloadOutputTypes } from "@smithy/types";
import { isBlob, isReadableStream, sdkStreamMixin } from "@smithy/util-stream";
import { request } from "http";
import { Readable } from "stream";

// check all types. needs to join nodejs and browser together
export function joinStreams(
  streams: StreamingBlobPayloadOutputTypes[],
  transferInfo?: {
    request: DownloadRequest;
    dispatchEvent?: (event: Event) => boolean;
    totalBytes?: number;
  }
): StreamingBlobPayloadOutputTypes {
  // console.log("Is Readable Stream: ");
  // console.log(isReadableStream(streams[0]));

  if (streams.length === 1) {
    return streams[0];
  } else if (isReadableStream(streams[0])) {
    const newReadableStream = new ReadableStream({
      async start(controller) {
        for await (const chunk of iterateStreams(streams)) {
          controller.enqueue(chunk);
        }
        controller.close();
      },
    });
    return sdkStreamMixin(newReadableStream);
  } else if (isBlob(streams[0])) {
    throw new Error("Blob not supported yet");
  } else {
    return sdkStreamMixin(Readable.from(iterateStreams(streams, transferInfo)));
  }
}

export async function* iterateStreams(
  streams: StreamingBlobPayloadOutputTypes[],
  transferInfo?: {
    request: DownloadRequest;
    dispatchEvent?: (event: Event) => boolean;
    totalBytes?: number;
  }
): AsyncIterable<StreamingBlobPayloadOutputTypes, void, void> {
  let bytesTransferred = 0;
  for (const stream of streams) {
    if (isReadableStream(stream)) {
      const reader = stream.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        yield value;
        bytesTransferred += value.byteLength;
      }
      reader.releaseLock();
    } else if (isBlob(stream)) {
      throw new Error("Blob not supported yet");
    } else if (stream instanceof Readable) {
      for await (const chunk of stream) {
        yield chunk;
        const chunkSize = Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk);
        bytesTransferred += chunkSize;

        if (transferInfo?.request && transferInfo?.dispatchEvent) {
          transferInfo.dispatchEvent(
            Object.assign(new Event("bytesTransferred"), {
              request: transferInfo.request,
              snapshot: {
                transferredBytes: bytesTransferred,
                totalBytes: transferInfo.totalBytes,
              },
            })
          );
        }
      }
    } else {
      throw new Error("unhandled stream type", stream);
    }
  }
}
