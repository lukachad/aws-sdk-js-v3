export function isWebStream(stream: unknown): stream is ReadableStream | Blob {
  return (
    (typeof ReadableStream !== "undefined" && stream instanceof ReadableStream) ||
    (typeof Blob !== "undefined" && stream instanceof Blob)
  );
}
