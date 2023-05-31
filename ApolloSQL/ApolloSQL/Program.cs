using System;
using System.Net;
using System.IO;
using System.IO.Compression;
using System.Data.SQLite;
using System.Text;
using System.Globalization;

namespace DemoApplication
{
    public class Program
    {
        public static void Main()
        {
            string url = "https://github.com/datablist/sample-csv-files/raw/main/files/people/people-1000.csv";
            string tempFolderPath = Directory.GetCurrentDirectory(); //current working forder
            string csvFilePath = Path.Combine(tempFolderPath, "people-1000.csv");
            string dbPath = "Users.db";
            string inputFormat = "yyyy-MM-dd"; //original date format
            string outputFormat = "yyyy/MM/dd"; //customize designed date format
            string query = @"CREATE TABLE IF NOT EXISTS Users (
                                                [Index] INTEGER,
                                                User_Id TEXT PRIMARY KEY,
                                                First_Name TEXT,
                                                Last_Name TEXT,
                                                Sex TEXT,
                                                Email TEXT,
                                                Phone TEXT,
                                                Date_of_Birth DATE,
                                                Job_Title TEXT
                                            )";

            //step 1: create users table 
            createTable(dbPath, query);
            Console.WriteLine("Completed: users database created");
            // Step 2: Download the csv file
            downloadFile(url, csvFilePath);
            Console.WriteLine("Completed: csv file saved to diectory");

            // Step 2.5: Unzip the file
            //unzipFile(zipFilePath, tempFolderPath);

            // Step 3: Read the csv file and insert data into the table
            insertDataToDb(csvFilePath, dbPath, inputFormat, outputFormat);
            Console.WriteLine("Completed: data entered to users.db");

        }

        //FUNCTION: create users db
        static void createTable(string dbPath, string query)
        {

            if (File.Exists(dbPath))
            {
                // Delete the existing database file
                File.Delete(dbPath);
            }
            // Create the SQLite database file
            SQLiteConnection.CreateFile(dbPath);

            string connectionString = $"Data Source={dbPath};Version=3;";
            using (var conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (var createDbCommand = new SQLiteCommand(query, conn))
                {
                    createDbCommand.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

        //FUNCTION: Download zip file from url
        static void downloadFile(string url, string savePath)
        {
            using (WebClient webClient = new WebClient())
            {
                webClient.DownloadFile(url, savePath);
            }
        }


        //FUNCTION: unzip file.zip and save to temp folder
        //static void unzipFile(string zipFilePath, string folderPath)
        //{
        //    ZipFile.ExtractToDirectory(zipFilePath, folderPath);
        //}


        //FUNCTION:Connect to SQLite
        static void insertDataToDb(string csvFilePath, string dbPath, string inputFormat, string outputFormat)
        {
            string connectionString = $"Data Source={dbPath};Version=3;";
            using (var conn = new SQLiteConnection(connectionString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                using (StreamReader reader = new StreamReader(csvFilePath, Encoding.Default))
                {
                    reader.ReadLine(); // skip the first line 
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(',');

                        // Set the SQL command text (with parameter placeholder @)
                        cmd.CommandText = @" INSERT INTO Users (
                        [Index], User_Id, First_Name, Last_Name, Sex, Email, Phone, Date_Of_Birth, Job_Title)
                    VALUES (@Index, @UserId, @FirstName, @LastName, @Sex, @Email, @Phone, @DateOfBirth, @JobTitle)";

                        // Clear the command parameters
                        cmd.Parameters.Clear();

                        // Add parameters with corresponding values
                        cmd.Parameters.AddWithValue("@Index", values[0]);
                        cmd.Parameters.AddWithValue("@UserId", values[1]);
                        cmd.Parameters.AddWithValue("@FirstName", values[2]);
                        cmd.Parameters.AddWithValue("@LastName", values[3]);
                        cmd.Parameters.AddWithValue("@Sex", values[4]);
                        cmd.Parameters.AddWithValue("@Email", values[5]);
                        cmd.Parameters.AddWithValue("@Phone", values[6]);


                        // a. Parse the date field using the specified date format
                        DateTime dateOfBirth = DateTime.ParseExact(values[7], inputFormat, CultureInfo.InvariantCulture, DateTimeStyles.None);
                        string output = dateOfBirth.ToString(outputFormat);
                        cmd.Parameters.AddWithValue("@DateOfBirth", output);

                        
                        // b. unify the jobtitle format
                        var job = values[8]; //type: string
                        //Console.WriteLine(job[0]);
                        //if start with quote
                        if (job[0] == '\'' || (job[0] == '\"')) {
                            job = job.Substring(1);
                        }
                        //if end with quote
                        if (job[job.Length-1] == '\'' || (job[job.Length-1] == '\"'))
                        {
                            job = job.Substring(0,job.Length-1);
                        }
                        //Console.WriteLine(job[0]);

                        //As required, use double quote
                        cmd.Parameters.AddWithValue("@JobTitle", $"\"{job}\"");


                        cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                    }
                    conn.Close();
                }

            }

        }

    }

}


