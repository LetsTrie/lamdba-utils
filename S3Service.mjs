import {
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { sendErrorResponse } from "./utils.mjs";

let Upload;
let archiver;
let PassThrough;

class S3Service {
  constructor() {
    this.s3Client = new S3Client({});
  }

  getClient() {
    return this.s3Client;
  }

  async getS3Object(
    Bucket,
    Key,
    options = { transformResponse: true, transformFn: null }
  ) {
    try {
      const params = { Bucket, Key };
      const command = new GetObjectCommand(params);
      const response = await this.s3Client.send(command);

      if (options.transformResponse) {
        if (typeof options.transformFn === "function") {
          return await options.transformFn(response.Body);
        }
        return await response.Body.transformToString();
      }
      return response.Body;
    } catch (error) {
      console.error("Error getting S3 object: ", error);
      return null;
    }
  }

  async putS3Object(Bucket, Key, Body) {
    try {
      const params = { Bucket, Key, Body };
      const command = new PutObjectCommand(params);
      const response = await this.s3Client.send(command);
      return response;
    } catch (error) {
      console.error("Error putting S3 object: ", error);
      throw error;
    }
  }

  async checkIfFileExists(Bucket, Key) {
    try {
      await this.s3Client.send(new HeadObjectCommand({ Bucket, Key }));
      return true;
    } catch (error) {
      if (error.name === "NotFound") return false;
      if (error.$metadata?.httpStatusCode === 404) return false;

      console.error("Error checking if file exists in S3: ", error);
      throw error;
    }
  }

  async generateGetPresignedUrl(Bucket, Key, expiresIn, downloadFilename) {
    try {
      const params = { Bucket, Key };

      const isFileExists = await this.checkIfFileExists(Bucket, Key);
      if (!isFileExists) {
        return sendErrorResponse(
          404,
          "The specified key does not exist in the bucket."
        );
      }

      if (downloadFilename) {
        params[
          "ResponseContentDisposition"
        ] = `inline; filename="${encodeURIComponent(downloadFilename)}"`;
      }

      const command = new GetObjectCommand(params);
      const presignedUrl = await getSignedUrl(this.s3Client, command, {
        expiresIn,
      });

      return {
        statusCode: 200,
        body: JSON.stringify({ presignedUrl }),
      };
    } catch (error) {
      console.error("Error generating get presigned URL:", error);
      return sendErrorResponse(500, error.message);
    }
  }

  async generatePutPresignedUrl(Bucket, Key, expiresIn) {
    try {
      const params = { Bucket, Key };

      const command = new PutObjectCommand(params);
      const presignedUrl = await getSignedUrl(this.s3Client, command, {
        expiresIn,
      });

      return {
        statusCode: 200,
        body: JSON.stringify({ presignedUrl }),
      };
    } catch (error) {
      console.error("Error generating put presigned URL:", error);
      return sendErrorResponse(500, error.message);
    }
  }

  async getWritableStreamFromS3(Bucket, Key) {
    const streamLib = await import("stream");
    const awsSdkLibStorage = await import("@aws-sdk/lib-storage");

    PassThrough = streamLib.PassThrough;
    Upload = awsSdkLibStorage.Upload;

    let passThroughStream = new PassThrough();
    const params = {
      Bucket,
      Key,
      Body: passThroughStream,
    };
    const uploadOperation = new Upload({
      client: this.s3Client,
      params,
    }).done();
    return { uploadOperation, passThroughStream };
  }

  async generateAndStreamZipfileToS3(bucket, zipFileKey, sourceKey) {
    try {
      archiver = await import("archiver");

      const zipArchive = archiver("zip", { zlib: { level: 9 } });

      const s3SourceStream = await this.getS3Object(bucket, sourceKey, {
        transformResponse: false,
      });
      zipArchive.append(s3SourceStream, { name: sourceKey });

      const { uploadOperation, passThroughStream } =
        await this.getWritableStreamFromS3(bucket, zipFileKey);

      zipArchive.pipe(passThroughStream);
      zipArchive.finalize();

      await uploadOperation;
    } catch (error) {
      console.error(`Error in generateAndStreamZipfileToS3: ${error.message}`);
    }
  }
}

export default S3Service;
