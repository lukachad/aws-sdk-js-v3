// smithy-typescript generated code
import { ExceptionOptionType as __ExceptionOptionType } from "@smithy/smithy-client";

import { DocumentType as __DocumentType } from "@smithy/types";

import { S3VectorsServiceException as __BaseException } from "./S3VectorsServiceException";

/**
 * <p>Access denied.</p>
 * @public
 */
export class AccessDeniedException extends __BaseException {
  readonly name: "AccessDeniedException" = "AccessDeniedException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<AccessDeniedException, __BaseException>) {
    super({
      name: "AccessDeniedException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, AccessDeniedException.prototype);
  }
}

/**
 * <p>The request failed because a vector bucket name or a vector index name already exists. Vector bucket names must be unique within your Amazon Web Services account for each Amazon Web Services Region. Vector index names must be unique within your vector bucket. Choose a different vector bucket name or vector index name, and try again.</p>
 * @public
 */
export class ConflictException extends __BaseException {
  readonly name: "ConflictException" = "ConflictException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<ConflictException, __BaseException>) {
    super({
      name: "ConflictException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, ConflictException.prototype);
  }
}

/**
 * @public
 * @enum
 */
export const DataType = {
  FLOAT32: "float32",
} as const;

/**
 * @public
 */
export type DataType = (typeof DataType)[keyof typeof DataType];

/**
 * @public
 * @enum
 */
export const DistanceMetric = {
  COSINE: "cosine",
  EUCLIDEAN: "euclidean",
} as const;

/**
 * @public
 */
export type DistanceMetric = (typeof DistanceMetric)[keyof typeof DistanceMetric];

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The metadata configuration for a vector index.</p>
 * @public
 */
export interface MetadataConfiguration {
  /**
   * <p>Non-filterable metadata keys allow you to enrich vectors with additional context during storage and retrieval. Unlike default metadata keys, these keys can’t be used as query filters. Non-filterable metadata keys can be retrieved but can’t be searched, queried, or filtered. You can access non-filterable metadata keys of your vectors after finding the vectors. For more information about non-filterable metadata keys, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-vectors.html">Vectors</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html">Limitations and restrictions</a> in the <i>Amazon S3 User Guide</i>. </p>
   * @public
   */
  nonFilterableMetadataKeys: string[] | undefined;
}

/**
 * @public
 */
export interface CreateIndexInput {
  /**
   * <p>The name of the vector bucket to create the vector index in. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector bucket to create the vector index in.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;

  /**
   * <p>The name of the vector index to create. </p>
   * @public
   */
  indexName: string | undefined;

  /**
   * <p>The data type of the vectors to be inserted into the vector index. </p>
   * @public
   */
  dataType: DataType | undefined;

  /**
   * <p>The dimensions of the vectors to be inserted into the vector index. </p>
   * @public
   */
  dimension: number | undefined;

  /**
   * <p>The distance metric to be used for similarity search. </p>
   * @public
   */
  distanceMetric: DistanceMetric | undefined;

  /**
   * <p>The metadata configuration for the vector index. </p>
   * @public
   */
  metadataConfiguration?: MetadataConfiguration | undefined;
}

/**
 * @public
 */
export interface CreateIndexOutput {}

/**
 * <p>The request failed due to an internal server error.</p>
 * @public
 */
export class InternalServerException extends __BaseException {
  readonly name: "InternalServerException" = "InternalServerException";
  readonly $fault: "server" = "server";
  $retryable = {};
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<InternalServerException, __BaseException>) {
    super({
      name: "InternalServerException",
      $fault: "server",
      ...opts,
    });
    Object.setPrototypeOf(this, InternalServerException.prototype);
  }
}

/**
 * <p>The request was rejected because the specified resource can't be found.</p>
 * @public
 */
export class NotFoundException extends __BaseException {
  readonly name: "NotFoundException" = "NotFoundException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<NotFoundException, __BaseException>) {
    super({
      name: "NotFoundException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, NotFoundException.prototype);
  }
}

/**
 * <p>Your request exceeds a service quota. </p>
 * @public
 */
export class ServiceQuotaExceededException extends __BaseException {
  readonly name: "ServiceQuotaExceededException" = "ServiceQuotaExceededException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<ServiceQuotaExceededException, __BaseException>) {
    super({
      name: "ServiceQuotaExceededException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, ServiceQuotaExceededException.prototype);
  }
}

/**
 * <p>The service is unavailable. Wait briefly and retry your request. If it continues to fail, increase your waiting time between retries.</p>
 * @public
 */
export class ServiceUnavailableException extends __BaseException {
  readonly name: "ServiceUnavailableException" = "ServiceUnavailableException";
  readonly $fault: "server" = "server";
  $retryable = {};
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<ServiceUnavailableException, __BaseException>) {
    super({
      name: "ServiceUnavailableException",
      $fault: "server",
      ...opts,
    });
    Object.setPrototypeOf(this, ServiceUnavailableException.prototype);
  }
}

/**
 * <p>The request was denied due to request throttling.</p>
 * @public
 */
export class TooManyRequestsException extends __BaseException {
  readonly name: "TooManyRequestsException" = "TooManyRequestsException";
  readonly $fault: "client" = "client";
  $retryable = {
    throttling: true,
  };
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<TooManyRequestsException, __BaseException>) {
    super({
      name: "TooManyRequestsException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, TooManyRequestsException.prototype);
  }
}

/**
 * <p>Contains information about a validation exception.</p>
 * @public
 */
export interface ValidationExceptionField {
  /**
   * <p>A path about the validation exception.</p>
   * @public
   */
  path: string | undefined;

  /**
   * <p>A message about the validation exception.</p>
   * @public
   */
  message: string | undefined;
}

/**
 * <p>The requested action isn't valid.</p>
 * @public
 */
export class ValidationException extends __BaseException {
  readonly name: "ValidationException" = "ValidationException";
  readonly $fault: "client" = "client";
  /**
   * <p>A list of specific validation failures that are encountered during input processing. Each entry in the list contains a path to the field that failed validation and a detailed message that explains why the validation failed. To satisfy multiple constraints, a field can appear multiple times in this list if it failed. You can use the information to identify and fix the specific validation issues in your request.</p>
   * @public
   */
  fieldList?: ValidationExceptionField[] | undefined;

  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<ValidationException, __BaseException>) {
    super({
      name: "ValidationException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, ValidationException.prototype);
    this.fieldList = opts.fieldList;
  }
}

/**
 * @public
 * @enum
 */
export const SseType = {
  AES256: "AES256",
  AWS_KMS: "aws:kms",
} as const;

/**
 * @public
 */
export type SseType = (typeof SseType)[keyof typeof SseType];

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The encryption configuration for a vector bucket. By default, if you don't specify, all new vectors in Amazon S3 vector buckets use server-side encryption with Amazon S3 managed keys (SSE-S3), specifically <code>AES256</code>.</p>
 * @public
 */
export interface EncryptionConfiguration {
  /**
   * <p>The server-side encryption type to use for the encryption configuration of the vector bucket. By default, if you don't specify, all new vectors in Amazon S3 vector buckets use server-side encryption with Amazon S3 managed keys (SSE-S3), specifically <code>AES256</code>.</p>
   * @public
   */
  sseType?: SseType | undefined;

  /**
   * <p>Amazon Web Services Key Management Service (KMS) customer managed key ID to use for the encryption configuration. This parameter is allowed if and only if <code>sseType</code> is set to <code>aws:kms</code>.</p> <p>To specify the KMS key, you must use the format of the KMS key Amazon Resource Name (ARN).</p> <p>For example, specify Key ARN in the following format: <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
   * @public
   */
  kmsKeyArn?: string | undefined;
}

/**
 * @public
 */
export interface CreateVectorBucketInput {
  /**
   * <p>The name of the vector bucket to create. </p>
   * @public
   */
  vectorBucketName: string | undefined;

  /**
   * <p>The encryption configuration for the vector bucket. By default, if you don't specify, all new vectors in Amazon S3 vector buckets use server-side encryption with Amazon S3 managed keys (SSE-S3), specifically <code>AES256</code>. </p>
   * @public
   */
  encryptionConfiguration?: EncryptionConfiguration | undefined;
}

/**
 * @public
 */
export interface CreateVectorBucketOutput {}

/**
 * @public
 */
export interface DeleteIndexInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index to delete. </p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index to delete.</p>
   * @public
   */
  indexArn?: string | undefined;
}

/**
 * @public
 */
export interface DeleteIndexOutput {}

/**
 * @public
 */
export interface DeleteVectorBucketInput {
  /**
   * <p>The name of the vector bucket to delete.</p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The ARN of the vector bucket to delete.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;
}

/**
 * @public
 */
export interface DeleteVectorBucketOutput {}

/**
 * @public
 */
export interface DeleteVectorBucketPolicyInput {
  /**
   * <p>The name of the vector bucket to delete the policy from.</p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The ARN of the vector bucket to delete the policy from.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;
}

/**
 * @public
 */
export interface DeleteVectorBucketPolicyOutput {}

/**
 * @public
 */
export interface DeleteVectorsInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index that contains a vector you want to delete.</p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index that contains a vector you want to delete.</p>
   * @public
   */
  indexArn?: string | undefined;

  /**
   * <p>The keys of the vectors to delete. </p>
   * @public
   */
  keys: string[] | undefined;
}

/**
 * @public
 */
export interface DeleteVectorsOutput {}

/**
 * <p>The specified Amazon Web Services KMS key isn't enabled.</p>
 * @public
 */
export class KmsDisabledException extends __BaseException {
  readonly name: "KmsDisabledException" = "KmsDisabledException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<KmsDisabledException, __BaseException>) {
    super({
      name: "KmsDisabledException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, KmsDisabledException.prototype);
  }
}

/**
 * <p>The request was rejected for one of the following reasons: </p> <ul> <li> <p>The <code>KeyUsage</code> value of the KMS key is incompatible with the API operation.</p> </li> <li> <p>The encryption algorithm or signing algorithm specified for the operation is incompatible with the type of key material in the KMS key (<code>KeySpec</code>).</p> </li> </ul> <p>For more information, see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_Encrypt.html#API_Encrypt_Errors">InvalidKeyUsageException</a> in the <i>Amazon Web Services Key Management Service API Reference</i>.</p>
 * @public
 */
export class KmsInvalidKeyUsageException extends __BaseException {
  readonly name: "KmsInvalidKeyUsageException" = "KmsInvalidKeyUsageException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<KmsInvalidKeyUsageException, __BaseException>) {
    super({
      name: "KmsInvalidKeyUsageException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, KmsInvalidKeyUsageException.prototype);
  }
}

/**
 * <p>The key state of the KMS key isn't compatible with the operation.</p> <p>For more information, see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_Encrypt.html#API_Encrypt_Errors">KMSInvalidStateException</a> in the <i>Amazon Web Services Key Management Service API Reference</i>.</p>
 * @public
 */
export class KmsInvalidStateException extends __BaseException {
  readonly name: "KmsInvalidStateException" = "KmsInvalidStateException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<KmsInvalidStateException, __BaseException>) {
    super({
      name: "KmsInvalidStateException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, KmsInvalidStateException.prototype);
  }
}

/**
 * <p>The KMS key can't be found.</p>
 * @public
 */
export class KmsNotFoundException extends __BaseException {
  readonly name: "KmsNotFoundException" = "KmsNotFoundException";
  readonly $fault: "client" = "client";
  /**
   * @internal
   */
  constructor(opts: __ExceptionOptionType<KmsNotFoundException, __BaseException>) {
    super({
      name: "KmsNotFoundException",
      $fault: "client",
      ...opts,
    });
    Object.setPrototypeOf(this, KmsNotFoundException.prototype);
  }
}

/**
 * @public
 */
export interface GetIndexInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index.</p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index.</p>
   * @public
   */
  indexArn?: string | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector index.</p>
 * @public
 */
export interface Index {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName: string | undefined;

  /**
   * <p>The name of the vector index.</p>
   * @public
   */
  indexName: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector index.</p>
   * @public
   */
  indexArn: string | undefined;

  /**
   * <p>Date and time when the vector index was created. </p>
   * @public
   */
  creationTime: Date | undefined;

  /**
   * <p>The data type of the vectors inserted into the vector index. </p>
   * @public
   */
  dataType: DataType | undefined;

  /**
   * <p>The number of values in the vectors that are inserted into the vector index. </p>
   * @public
   */
  dimension: number | undefined;

  /**
   * <p>The distance metric to be used for similarity search. </p>
   * @public
   */
  distanceMetric: DistanceMetric | undefined;

  /**
   * <p>The metadata configuration for the vector index. </p>
   * @public
   */
  metadataConfiguration?: MetadataConfiguration | undefined;
}

/**
 * @public
 */
export interface GetIndexOutput {
  /**
   * <p>The attributes of the vector index.</p>
   * @public
   */
  index: Index | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The vector data in different formats.</p>
 * @public
 */
export type VectorData = VectorData.Float32Member | VectorData.$UnknownMember;

/**
 * @public
 */
export namespace VectorData {
  /**
   * <p>The vector data as 32-bit floating point numbers. The number of elements in this array must exactly match the dimension of the vector index where the operation is being performed.</p>
   * @public
   */
  export interface Float32Member {
    float32: number[];
    $unknown?: never;
  }

  /**
   * @public
   */
  export interface $UnknownMember {
    float32?: never;
    $unknown: [string, any];
  }

  export interface Visitor<T> {
    float32: (value: number[]) => T;
    _: (name: string, value: any) => T;
  }

  export const visit = <T>(value: VectorData, visitor: Visitor<T>): T => {
    if (value.float32 !== undefined) return visitor.float32(value.float32);
    return visitor._(value.$unknown[0], value.$unknown[1]);
  };
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector returned by the <code>GetVectors</code> operation.</p>
 * @public
 */
export interface GetOutputVector {
  /**
   * <p>The name of the vector. </p>
   * @public
   */
  key: string | undefined;

  /**
   * <p>The vector data of the vector. </p>
   * @public
   */
  data?: VectorData | undefined;

  /**
   * <p>Metadata about the vector. </p>
   * @public
   */
  metadata?: __DocumentType | undefined;
}

/**
 * @public
 */
export interface GetVectorBucketInput {
  /**
   * <p>The name of the vector bucket to retrieve information about.</p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The ARN of the vector bucket to retrieve information about.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector bucket. </p>
 * @public
 */
export interface VectorBucket {
  /**
   * <p>The name of the vector bucket. </p>
   * @public
   */
  vectorBucketName: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector bucket. </p>
   * @public
   */
  vectorBucketArn: string | undefined;

  /**
   * <p>Date and time when the vector bucket was created. </p>
   * @public
   */
  creationTime: Date | undefined;

  /**
   * <p>The encryption configuration for the vector bucket.</p>
   * @public
   */
  encryptionConfiguration?: EncryptionConfiguration | undefined;
}

/**
 * @public
 */
export interface GetVectorBucketOutput {
  /**
   * <p>The attributes of the vector bucket.</p>
   * @public
   */
  vectorBucket: VectorBucket | undefined;
}

/**
 * @public
 */
export interface GetVectorBucketPolicyInput {
  /**
   * <p>The name of the vector bucket.</p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The ARN of the vector bucket.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;
}

/**
 * @public
 */
export interface GetVectorBucketPolicyOutput {
  /**
   * <p>The <code>JSON</code> that defines the policy.</p>
   * @public
   */
  policy?: string | undefined;
}

/**
 * @public
 */
export interface GetVectorsInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index.</p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index.</p>
   * @public
   */
  indexArn?: string | undefined;

  /**
   * <p>The names of the vectors you want to return attributes for. </p>
   * @public
   */
  keys: string[] | undefined;

  /**
   * <p>Indicates whether to include the vector data in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnData?: boolean | undefined;

  /**
   * <p>Indicates whether to include metadata in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnMetadata?: boolean | undefined;
}

/**
 * @public
 */
export interface GetVectorsOutput {
  /**
   * <p>The attributes of the vectors.</p>
   * @public
   */
  vectors: GetOutputVector[] | undefined;
}

/**
 * @public
 */
export interface ListIndexesInput {
  /**
   * <p>The name of the vector bucket that contains the vector indexes. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The ARN of the vector bucket that contains the vector indexes.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;

  /**
   * <p>The maximum number of items to be returned in the response. </p>
   * @public
   */
  maxResults?: number | undefined;

  /**
   * <p>The previous pagination token. </p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>Limits the response to vector indexes that begin with the specified prefix.</p>
   * @public
   */
  prefix?: string | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>Summary information about a vector index.</p>
 * @public
 */
export interface IndexSummary {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName: string | undefined;

  /**
   * <p>The name of the vector index.</p>
   * @public
   */
  indexName: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector index.</p>
   * @public
   */
  indexArn: string | undefined;

  /**
   * <p>Date and time when the vector index was created. </p>
   * @public
   */
  creationTime: Date | undefined;
}

/**
 * @public
 */
export interface ListIndexesOutput {
  /**
   * <p>The next pagination token.</p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>The attributes of the vector indexes</p>
   * @public
   */
  indexes: IndexSummary[] | undefined;
}

/**
 * @public
 */
export interface ListVectorsInput {
  /**
   * <p>The name of the vector bucket. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index.</p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The Amazon resource Name (ARN) of the vector index.</p>
   * @public
   */
  indexArn?: string | undefined;

  /**
   * <p>The maximum number of vectors to return on a page.</p> <p>If you don't specify <code>maxResults</code>, the <code>ListVectors</code> operation uses a default value of 500.</p> <p>If the processed dataset size exceeds 1 MB before reaching the <code>maxResults</code> value, the operation stops and returns the vectors that are retrieved up to that point, along with a <code>nextToken</code> that you can use in a subsequent request to retrieve the next set of results.</p>
   * @public
   */
  maxResults?: number | undefined;

  /**
   * <p>Pagination token from a previous request. The value of this field is empty for an initial request.</p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>For a parallel <code>ListVectors</code> request, <code>segmentCount</code> represents the total number of vector segments into which the <code>ListVectors</code> operation will be divided. The value of <code>segmentCount</code> corresponds to the number of application workers that will perform the parallel <code>ListVectors</code> operation. For example, if you want to use four application threads to list vectors in a vector index, specify a <code>segmentCount</code> value of 4. </p> <p>If you specify a <code>segmentCount</code> value of 1, the <code>ListVectors</code> operation will be sequential rather than parallel.</p> <p>If you specify <code>segmentCount</code>, you must also specify <code>segmentIndex</code>.</p>
   * @public
   */
  segmentCount?: number | undefined;

  /**
   * <p>For a parallel <code>ListVectors</code> request, <code>segmentIndex</code> is the index of the segment from which to list vectors in the current request. It identifies an individual segment to be listed by an application worker. </p> <p>Segment IDs are zero-based, so the first segment is always 0. For example, if you want to use four application threads to list vectors in a vector index, then the first thread specifies a <code>segmentIndex</code> value of 0, the second thread specifies 1, and so on. </p> <p>The value of <code>segmentIndex</code> must be less than the value provided for <code>segmentCount</code>. </p> <p>If you provide <code>segmentIndex</code>, you must also provide <code>segmentCount</code>.</p>
   * @public
   */
  segmentIndex?: number | undefined;

  /**
   * <p>If true, the vector data of each vector will be included in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnData?: boolean | undefined;

  /**
   * <p>If true, the metadata associated with each vector will be included in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnMetadata?: boolean | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector returned by the <code>ListVectors</code> operation.</p>
 * @public
 */
export interface ListOutputVector {
  /**
   * <p>The name of the vector. </p>
   * @public
   */
  key: string | undefined;

  /**
   * <p>The vector data of the vector. </p>
   * @public
   */
  data?: VectorData | undefined;

  /**
   * <p>Metadata about the vector. </p>
   * @public
   */
  metadata?: __DocumentType | undefined;
}

/**
 * @public
 */
export interface ListVectorsOutput {
  /**
   * <p>Pagination token to be used in the subsequent request. The field is empty if no further pagination is required.</p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>Vectors in the current segment.</p>
   * @public
   */
  vectors: ListOutputVector[] | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector to add to a vector index.</p>
 * @public
 */
export interface PutInputVector {
  /**
   * <p>The name of the vector. The key uniquely identifies the vector in a vector index. </p>
   * @public
   */
  key: string | undefined;

  /**
   * <p>The vector data of the vector. </p> <p>Vector dimensions must match the dimension count that's configured for the vector index.</p> <ul> <li> <p>For the <code>cosine</code> distance metric, zero vectors (vectors containing all zeros) aren't allowed.</p> </li> <li> <p>For both <code>cosine</code> and <code>euclidean</code> distance metrics, vector data must contain only valid floating-point values. Invalid values such as NaN (Not a Number) or Infinity aren't allowed.</p> </li> </ul>
   * @public
   */
  data: VectorData | undefined;

  /**
   * <p>Metadata about the vector. All metadata entries undergo validation to ensure they meet the format requirements for size and data types.</p>
   * @public
   */
  metadata?: __DocumentType | undefined;
}

/**
 * @public
 */
export interface PutVectorsInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index where you want to write vectors. </p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index where you want to write vectors.</p>
   * @public
   */
  indexArn?: string | undefined;

  /**
   * <p>The vectors to add to a vector index. The number of vectors in a single request must not exceed the resource capacity, otherwise the request will be rejected with the error <code>ServiceUnavailableException</code> with the error message "Currently unable to handle the request".</p>
   * @public
   */
  vectors: PutInputVector[] | undefined;
}

/**
 * @public
 */
export interface PutVectorsOutput {}

/**
 * @public
 */
export interface QueryVectorsInput {
  /**
   * <p>The name of the vector bucket that contains the vector index. </p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The name of the vector index that you want to query. </p>
   * @public
   */
  indexName?: string | undefined;

  /**
   * <p>The ARN of the vector index that you want to query.</p>
   * @public
   */
  indexArn?: string | undefined;

  /**
   * <p>The number of results to return for each query.</p>
   * @public
   */
  topK: number | undefined;

  /**
   * <p>The query vector. Ensure that the query vector has the same dimension as the dimension of the vector index that's being queried. For example, if your vector index contains vectors with 384 dimensions, your query vector must also have 384 dimensions. </p>
   * @public
   */
  queryVector: VectorData | undefined;

  /**
   * <p>Metadata filter to apply during the query. For more information about metadata keys, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-metadata-filtering.html">Metadata filtering</a> in the <i>Amazon S3 User Guide</i>. </p>
   * @public
   */
  filter?: __DocumentType | undefined;

  /**
   * <p>Indicates whether to include metadata in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnMetadata?: boolean | undefined;

  /**
   * <p>Indicates whether to include the computed distance in the response. The default value is <code>false</code>.</p>
   * @public
   */
  returnDistance?: boolean | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>The attributes of a vector in the approximate nearest neighbor search.</p>
 * @public
 */
export interface QueryOutputVector {
  /**
   * <p>The key of the vector in the approximate nearest neighbor search.</p>
   * @public
   */
  key: string | undefined;

  /**
   * <p>The vector data associated with the vector, if requested. </p>
   * @public
   */
  data?: VectorData | undefined;

  /**
   * <p>The metadata associated with the vector, if requested.</p>
   * @public
   */
  metadata?: __DocumentType | undefined;

  /**
   * <p>The measure of similarity between the vector in the response and the query vector.</p>
   * @public
   */
  distance?: number | undefined;
}

/**
 * @public
 */
export interface QueryVectorsOutput {
  /**
   * <p>The vectors in the approximate nearest neighbor search.</p>
   * @public
   */
  vectors: QueryOutputVector[] | undefined;
}

/**
 * @public
 */
export interface ListVectorBucketsInput {
  /**
   * <p>The maximum number of vector buckets to be returned in the response. </p>
   * @public
   */
  maxResults?: number | undefined;

  /**
   * <p>The previous pagination token. </p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>Limits the response to vector buckets that begin with the specified prefix.</p>
   * @public
   */
  prefix?: string | undefined;
}

/**
 * <note> <p>Amazon S3 Vectors is in preview release for Amazon S3 and is subject to change.</p> </note> <p>Summary information about a vector bucket.</p>
 * @public
 */
export interface VectorBucketSummary {
  /**
   * <p>The name of the vector bucket. </p>
   * @public
   */
  vectorBucketName: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector bucket. </p>
   * @public
   */
  vectorBucketArn: string | undefined;

  /**
   * <p>Date and time when the vector bucket was created. </p>
   * @public
   */
  creationTime: Date | undefined;
}

/**
 * @public
 */
export interface ListVectorBucketsOutput {
  /**
   * <p>The element is included in the response when there are more buckets to be listed with pagination. </p>
   * @public
   */
  nextToken?: string | undefined;

  /**
   * <p>The list of vector buckets owned by the requester. </p>
   * @public
   */
  vectorBuckets: VectorBucketSummary[] | undefined;
}

/**
 * @public
 */
export interface PutVectorBucketPolicyInput {
  /**
   * <p>The name of the vector bucket.</p>
   * @public
   */
  vectorBucketName?: string | undefined;

  /**
   * <p>The Amazon Resource Name (ARN) of the vector bucket.</p>
   * @public
   */
  vectorBucketArn?: string | undefined;

  /**
   * <p>The <code>JSON</code> that defines the policy. For more information about bucket policies for S3 Vectors, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-bucket-policy.html">Managing vector bucket policies</a> in the <i>Amazon S3 User Guide</i>.</p>
   * @public
   */
  policy: string | undefined;
}

/**
 * @public
 */
export interface PutVectorBucketPolicyOutput {}
