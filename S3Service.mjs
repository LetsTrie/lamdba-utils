import {
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

let archiver;
let PassThrough;

const sendErrorResponse = (code, message) => {
  return {
    statusCode: code,
    body: JSON.stringify({
      message: message,
    }),
  };
};

class S3Service {
  /**
   * @param {Object} config - S3 client configuration options.
   */
  constructor(config = {}) {
    this.s3Client = new S3Client(config);
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
      const command = new PutObjectCommand({ Bucket, Key, Body });
      return await this.s3Client.send(command);
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
      const isFileExists = await this.checkIfFileExists(Bucket, Key);
      if (!isFileExists) {
        return sendErrorResponse(
          404,
          "The specified key does not exist in the bucket."
        );
      }

      const params = { Bucket, Key };
      if (downloadFilename) {
        params[
          "ResponseContentDisposition"
        ] = `inline; filename="${encodeURIComponent(downloadFilename)}"`;
      }

      const command = new GetObjectCommand(params);
      const preSignedUrl = await getSignedUrl(this.s3Client, command, {
        expiresIn,
      });

      return {
        statusCode: 200,
        body: JSON.stringify({ preSignedUrl }),
      };
    } catch (error) {
      console.error("Error generating get presigned URL:", error);
      return sendErrorResponse(500, error.message);
    }
  }

  async generatePutPresignedUrl(Bucket, Key, expiresIn) {
    try {
      const command = new PutObjectCommand({ Bucket, Key });
      const preSignedUrl = await getSignedUrl(this.s3Client, command, {
        expiresIn,
      });

      return {
        statusCode: 200,
        body: JSON.stringify({ preSignedUrl }),
      };
    } catch (error) {
      console.error("Error generating put presigned URL:", error);
      return sendErrorResponse(500, error.message);
    }
  }

  async getWritableStreamFromS3(Bucket, Key) {
    const streamLib = await import("stream");
    PassThrough = streamLib.PassThrough;

    let passThroughStream = new PassThrough();
    const uploadOperation = new Upload({
      client: this.s3Client,
      params: {
        Bucket,
        Key,
        Body: passThroughStream,
      },
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
      await zipArchive.finalize();

      await uploadOperation;
    } catch (error) {
      console.error(`Error in generateAndStreamZipfileToS3: ${error.message}`);
    }
  }
}

export default S3Service;
