import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  GetBucketVersioningCommand,
} from "npm:@aws-sdk/client-s3";
import { load } from "jsr:@std/dotenv";

await load({
  export: true,
});
const s3 = new S3Client({
  credentials: {
    accessKeyId: Deno.env.get("aws_access_key_id")!,
    secretAccessKey: Deno.env.get("aws_secret_access_key")!,
  },
  region: "ap-south-1",
});

if (import.meta.main) {
  /**
   * 1. create a versioned s3 bucket
   * 2. upload an object
   * 3. upload again on same key
   * 4. check the version tree in s3 console
   * 5. delete the object
   * 6. check the version tree again
   * 7. delete the (delete marker) with key and versionId
   */
  const bucket = "testbucket-1002";

  const versionedBucketExists = await checkBucket(bucket);
  if (versionedBucketExists) {
    console.log("Bucket already exists");
  }

  const key = "example/myimage";
  const filePath = "/Users/bontaramsonta/p/aws-training/scripts/myimage.png";
  const uploadResult = await uploadFile({
    bucket,
    key,
    filePath,
    metadata: {
      "Content-Type": "image/png",
    },
  });
  console.log({ uploadResult });

  const getResult = await getObject({ bucket, key });
  console.log({ getResult });

  const deleteResult = await deleteFileVersion({
    bucket,
    key,
    versionId: getResult.VersionId!,
  });
  console.log({ deleteResult });
}

/**
 * returns true if bucket exists with versioning, false otherwise
 * @example
 * const result = await checkBucket({
 *  bucket: "testbucket-1002"
 * })
 */
export async function checkBucket(bucket: string) {
  const bucketVersioning = new GetBucketVersioningCommand({ Bucket: bucket });
  const { Status } = await s3.send(bucketVersioning);
  if (Status === undefined) {
    console.error("{GetBucketVersioningCommand} Status undefined");
    return false;
  }
  if (Status === "Enabled") {
    return true;
  }
  return false;
}

/**
 * upload a file to s3 bucket
 * @example
 * const result = await uploadFile({
 *  bucket: "testbucket-1002",
 *  key: "example/myimage",
 *  metadata: {
 *    "Content-Type": "image/png",
 *  }
 * })
 */
export function uploadFile(options: {
  bucket: string;
  key: string;
  filePath: string;
  metadata: Record<string, string>;
}) {
  const file = Deno.readFileSync(options.filePath);
  const putObject = new PutObjectCommand({
    Bucket: options.bucket,
    Key: options.key,
    Body: file,
    Metadata: options.metadata,
  });
  return s3.send(putObject);
}

/**
 * @example
 * const result = await deleteFile({
 *  bucket: "testbucket-1002",
 *  key: "example/myimage"
 * })
 */
export function deleteFile(options: { bucket: string; key: string }) {
  const deleteObj = new DeleteObjectCommand({
    Bucket: options.bucket,
    Key: options.key,
  });
  return s3.send(deleteObj);
}

/**
 * @example
 * const result = await getObject({
 *  bucket: "testbucket-1002",
 *  key: "example/myimage"
 * })
 */
export function deleteFileVersion(options: {
  bucket: string;
  key: string;
  versionId: string;
}) {
  const deleteVersion = new DeleteObjectCommand({
    Bucket: options.bucket,
    Key: options.key,
    VersionId: options.versionId,
  });
  return s3.send(deleteVersion);
}

/**
 * const result = await getObject({
 *  bucket: "testbucket-1002",
 *  key: "example/myimage",
 *  versionId: "v-1234567890"
 * })
 */
export function getObject(options: {
  bucket: string;
  key: string;
  versionId?: string;
}) {
  const getObj = new GetObjectCommand({
    Bucket: options.bucket,
    Key: options.key,
    VersionId: options.versionId,
  });
  return s3.send(getObj);
}
