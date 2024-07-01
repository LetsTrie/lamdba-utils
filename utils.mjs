export const sendErrorResponse = (code, message) => {
  return {
    statusCode: code,
    body: JSON.stringify({
      message: message,
    }),
  };
};
