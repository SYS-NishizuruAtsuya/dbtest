package com.function;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

public class FetchStudentsFunction {

    @FunctionName("FetchStudentsFunction")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = { HttpMethod.GET }, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Fetching secrets from Azure Key Vault...");

        // Replace with your Key Vault URL
        String keyVaultUrl = "https://prodddsjaesdatakvault.vault.azure.net/";
        
        // Create a SecretClient using DefaultAzureCredential (Managed Identity)
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        // Retrieve the connection string from Key Vault (for Blob Storage)
        String connectionString = secretClient.getSecret("BlobStorageConnectionString").getValue();
        context.getLogger().info("Successfully retrieved connection string from Key Vault.");

        // PostgreSQL database credentials (update with correct VM private IP and credentials)
        String dbUrl = "jdbc:postgresql://10.5.0.4:5432/testdb";
        String dbUsername = "postgres"; // Replace with your PostgreSQL username
        String dbPassword = "Pass8737!!!!";  // Replace with your PostgreSQL password

        String containerName = "test-proddirectdebitsystemcontainer";
        String blobName = "students_data.csv";

        try {
            // Fetch data from PostgreSQL database
            List<String[]> studentData = fetchDataFromPostgreSQL(dbUrl, dbUsername, dbPassword, context);

            // Generate CSV content in memory and upload to Azure Blob Storage
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            generateCSV(studentData, byteArrayOutputStream);

            // Upload to Azure Blob Storage
            BlobContainerClient containerClient = new BlobContainerClientBuilder()
                    .connectionString(connectionString)
                    .containerName(containerName)
                    .buildClient();

            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.upload(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), byteArrayOutputStream.size(), true);

            return request.createResponseBuilder(HttpStatus.OK)
                    .body("CSV file uploaded successfully.")
                    .build();

        } catch (IOException e) {
            context.getLogger().severe("Error occurred: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage())
                    .build();
        }
    }

    private List<String[]> fetchDataFromPostgreSQL(String dbUrl, String dbUsername, String dbPassword, ExecutionContext context) throws IOException {
        List<String[]> data = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT id, name FROM students"); // Modify the query based on your table

            while (resultSet.next()) {
                String id = resultSet.getString("id");
                String name = resultSet.getString("name");
                data.add(new String[]{id, name});
            }
        } catch (Exception e) {
            context.getLogger().severe("Error connecting to the PostgreSQL database: " + e.getMessage());
            throw new IOException("Error connecting to the database", e);
        }
        return data;
    }

    private void generateCSV(List<String[]> data, ByteArrayOutputStream outputStream) throws IOException {
        CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(outputStream), CSVFormat.DEFAULT.withHeader("ID", "Name"));

        for (String[] row : data) {
            csvPrinter.printRecord((Object[]) row);
        }

        csvPrinter.flush();
        csvPrinter.close();
    }
}
