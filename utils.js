import {
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

class S3Service {
  constructor() {
    this.s3Client = new S3Client({});
  }

  sendErrorResponse(code, message) {
    return {
      statusCode: code,
      body: JSON.stringify({
        message: message,
      }),
    };
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
      throw error;
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
      if (
        error.name === "NotFound" ||
        error.$metadata?.httpStatusCode === 404
      ) {
        return false;
      }
      console.error("Error checking if file exists in S3: ", error);
      throw error;
    }
  }

  async generateGetPresignedUrl(Bucket, Key, expiresIn, downloadFilename) {
    try {
      const params = { Bucket, Key };

      // Check if the object exists
      const isFileExists = await this.checkIfFileExists(Bucket, Key);
      if (!isFileExists) {
        return this.sendErrorResponse(
          404,
          "The specified key does not exist in the bucket."
        );
      }

      if (downloadFilename) {
        params[
          "ResponseContentDisposition"
        ] = `inline; filename="${encodeURIComponent(downloadFilename)}"`;
      }

      // Generate pre-signed URL
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
      return this.sendErrorResponse(500, error.message);
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
      return this.sendErrorResponse(500, error.message);
    }
  }
}

export default S3Service;
