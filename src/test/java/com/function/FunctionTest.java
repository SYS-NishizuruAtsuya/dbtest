package com.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;

public class FunctionTest {

    // Test for the Function that retrieves data and creates a CSV file
    @Test
    public void testFetchStudentsFunction() throws Exception {
        // Setup
        @SuppressWarnings("unchecked")
        final HttpRequestMessage<Optional<String>> req = mock(HttpRequestMessage.class);

        // Mock query parameters (if needed, adjust this based on your query params)
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("name", "Azure");
        doReturn(queryParams).when(req).getQueryParameters();

        final Optional<String> queryBody = Optional.empty();
        doReturn(queryBody).when(req).getBody();

        // Mock response builder creation
        doAnswer(invocation -> {
            HttpStatus status = (HttpStatus) invocation.getArguments()[0];
            return new HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
        }).when(req).createResponseBuilder(any(HttpStatus.class));

        // Mock the ExecutionContext
        final ExecutionContext context = mock(ExecutionContext.class);
        doReturn(Logger.getGlobal()).when(context).getLogger();

        // Mock database interaction (you should mock your PostgreSQL connection and ResultSet)
        // You could mock the behavior of the database connection here, but it's unnecessary for this simple test

        // Initialize the Function
        FetchStudentsFunction function = new FetchStudentsFunction();

        // Invoke the function
        HttpResponseMessage ret = function.run(req, context);

        // Verify the HTTP status returned by the function
        assertEquals(HttpStatus.OK, ret.getStatus());

        // Since the CSV file is uploaded to Azure Blob Storage, we cannot test it locally.
        // If you want to test the interaction with Blob Storage, mock BlobClient and verify the upload.

        // Optional: Verify the upload to Blob Storage (mocking BlobClient)
        // Example:
        // verify(blobClient, times(1)).upload(any(ByteArrayInputStream.class), anyLong(), eq(true));

    }
}
