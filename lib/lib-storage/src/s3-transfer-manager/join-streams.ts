import { StreamingBlobPayloadOutputTypes } from "@smithy/types";
import { isBlob, isReadableStream, sdkStreamMixin } from "@smithy/util-stream";
import { Readable } from "stream";

// check all types. needs to join nodejs and browser together
export function joinStreams(streams: StreamingBlobPayloadOutputTypes[]): StreamingBlobPayloadOutputTypes {
  console.log("Is Readable Stream: ");
  console.log(isReadableStream(streams[0]));

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
    return sdkStreamMixin(Readable.from(iterateStreams(streams)));
  }
}

export async function* iterateStreams(
  streams: StreamingBlobPayloadOutputTypes[]
): AsyncIterable<StreamingBlobPayloadOutputTypes, void, void> {
  for (const stream of streams) {
    if (isReadableStream(stream)) {
      const reader = (stream as ReadableStream).getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          yield value;
        }
      } finally {
        reader.releaseLock();
      }
    } else if (isBlob(stream)) {
      throw new Error("Blob not supported yet");
    } else if (stream instanceof Readable) {
      for await (const chunk of stream) {
        yield chunk;
      }
    }
  }
}
