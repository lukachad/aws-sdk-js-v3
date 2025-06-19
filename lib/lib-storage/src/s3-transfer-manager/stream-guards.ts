import { Readable } from "node:stream";

// * confirm if filestream fits here *
export function isNodeStream(stream: unknown): stream is Readable {
  return stream instanceof Readable || (typeof stream === "object" && stream !== null && "pipe" in stream);
}
