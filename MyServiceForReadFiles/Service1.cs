using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using System.Data.SqlClient;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;

namespace MyServiceForReadFiles
{
    [DataContract]
    public class IdAndText
    {
        [DataMember]
        public string ID { get; set; }
        [DataMember]
        public string Text { get; set; }
    }

    [DataContract]
    class IdAndImage
    {
        [DataMember]
        public string ID { get; set; }
        [DataMember]
        public string[] Image { get; set; }
    }

    [DataContract]
    class IdAndUrl
    {
        [DataMember]
        public string ID { get; set; }
        [DataMember]
        public string[] Url { get; set; }
    }

    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Thread thread0 = new Thread(new ThreadStart(MakeThreads));
            thread0.Start();
        }

        protected override void OnStop()
        {
        }

        public void MakeThreads()
        {
            string filePath1 = @"C:\Users\Samsung\source\repos\Selenium\Selenium\f1.json";
            string filePath2 = @"C:\Users\Samsung\source\repos\Selenium\Selenium\f2.json";
            string filePath3 = @"C:\Users\Samsung\source\repos\Selenium\Selenium\f3.json";
            while (true)
            {

                Thread thread1 = new Thread(()=> ReadFileToWriteText(filePath1))
                {
                    Name = "thread1",
                    //Priority = ThreadPriority.AboveNormal
                };

                Thread thread2 = new Thread(() => ReadFileToWriteImage(filePath2))
                {
                    Name = "thread2",
                    //Priority = ThreadPriority.AboveNormal
                };

                Thread thread3 = new Thread(() => ReadFileToWriteUrl(filePath3))//new ParameterizedThreadStart(ReadFile))
                {
                    Name = "thread3",
                    //Priority = ThreadPriority.AboveNormal
                };

                thread1.Start();
                thread2.Start();
                thread3.Start();

                thread1.Join();
                thread2.Join();
                thread3.Join();

                Thread.Sleep(1000);
            }
        }


        public void ReadFileToWriteText(object obj)
        {
            string filePath = (string)obj;

            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Samsung\source\repos\MyServiceForReadFiles\MyServiceForReadFiles\NewsDataBase.mdf;Integrated Security=True");
            connection.Open();            

            IdAndText[] fromFile = null;

            DataContractJsonSerializer jsonFormatter = new DataContractJsonSerializer(typeof(IdAndText[]));

            MyMonitorForService.Lock(filePath);            

            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                fromFile = (IdAndText[])jsonFormatter.ReadObject(fs);
            }
            MyMonitorForService.Unlock(filePath);

            //проверяем на совпадение в БД по Id и наличию текста
            if (fromFile.Length > 0)
            {
                for (int i = 0; i < fromFile.Length; i++)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM News WHERE Id = '" + fromFile[i].ID + "'", connection);
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);

                    //Если совпадений по id в базе данных не найдено
                    if (dt.Rows.Count == 0)
                    {
                        //пытаемся добавить новую строку (т.к. в это время это может сделать другой поток)
                        try
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "INSERT INTO News (Id, Text)VALUES(@Id, @Text)";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Text", fromFile[i].Text);
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }

                        catch//обновляем строку с существующим id (если строку с таким id успел добавить другой поток)
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Text = @Text WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Text", fromFile[i].Text);
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        string text = ds.Tables[0].Rows[0][1].ToString();

                        if (text == "")
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Text = @Text WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Text", fromFile[i].Text);
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            connection.Close();
        }

        public void ReadFileToWriteImage(object obj)
        {
            string filePath = (string)obj;

            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Samsung\source\repos\MyServiceForReadFiles\MyServiceForReadFiles\NewsDataBase.mdf;Integrated Security=True");
            connection.Open();

            IdAndImage[] fromFile = null;

            DataContractJsonSerializer jsonFormatter = new DataContractJsonSerializer(typeof(IdAndImage[]));

            MyMonitorForService.Lock(filePath);

            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                fromFile = (IdAndImage[])jsonFormatter.ReadObject(fs);
            }
            MyMonitorForService.Unlock(filePath);

            //проверяем на совпадение в БД по Id и наличию текста
            if (fromFile.Length > 0)
            {
                for (int i = 0; i < fromFile.Length; i++)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM News WHERE Id = '" + fromFile[i].ID + "'", connection);
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);

                    //Если совпадений по id в базе данных не найдено
                    if (dt.Rows.Count == 0)
                    {
                        //пытаемся добавить новую строку (т.к. в это время это может сделать другой поток)
                        try
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "INSERT INTO News (Id, Image)VALUES(@Id, @Image)";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Image", ArrayToString(fromFile[i].Image));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                        catch//обновляем строку с существующим id (если строку с таким id успел добавить другой поток)
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Image = @Image WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Image", ArrayToString(fromFile[i].Image));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        string image = ds.Tables[0].Rows[0][2].ToString();

                        if (image == "")
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Image = @Image WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Image", ArrayToString(fromFile[i].Image));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            connection.Close();
        }

        public void ReadFileToWriteUrl(object obj)
        {
            string filePath = (string)obj;

            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Samsung\source\repos\MyServiceForReadFiles\MyServiceForReadFiles\NewsDataBase.mdf;Integrated Security=True");
            connection.Open();

            IdAndUrl[] fromFile = null;

            DataContractJsonSerializer jsonFormatter = new DataContractJsonSerializer(typeof(IdAndUrl[]));

            MyMonitorForService.Lock(filePath);

            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                fromFile = (IdAndUrl[])jsonFormatter.ReadObject(fs);
            }
            MyMonitorForService.Unlock(filePath);

            //проверяем на совпадение в БД по Id и наличию текста
            if (fromFile.Length > 0)
            {
                for (int i = 0; i < fromFile.Length; i++)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM News WHERE Id = '" + fromFile[i].ID + "'", connection);
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);

                    //Если совпадений по id в базе данных не найдено
                    if (dt.Rows.Count == 0)
                    {
                        //пытаемся добавить новую строку (т.к. в это время это может сделать другой поток)
                        try
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "INSERT INTO News(Id, Url)VALUES(@Id, @Url)";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Url", ArrayToString(fromFile[i].Url));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                        catch//обновляем строку с существующим id (если строку с таким id успел добавить другой поток)
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Url = @Url WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Url", ArrayToString(fromFile[i].Url));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        string url = ds.Tables[0].Rows[0][3].ToString();

                        if (url == "")
                        {
                            var command = connection.CreateCommand();
                            command.CommandText = "UPDATE News SET Url = @Url WHERE Id = @Id";
                            command.Parameters.AddWithValue("Id", fromFile[i].ID);
                            command.Parameters.AddWithValue("Url", ArrayToString(fromFile[i].Url));
                            command.Connection = connection;

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            connection.Close();
        }

        public static string ArrayToString(string[] arr)
        {
            string text = null;
            for (int i = 0; i < arr.Length; i++)
            {
                text += (i + 1).ToString() + ") " + arr[i];
            }
            return text;
        }
    }

    public static class MyMonitorForService
    {
        private static Thread thread;

        static MyMonitorForService()
        {
            thread = Thread.CurrentThread;
        }

        public static void Lock(string filePath)
        {
            string lockFile = @"" + new FileInfo(filePath).DirectoryName + "/lock_" + new FileInfo(filePath).Name.Substring(0, 2) + ".txt";

            int n = 0;
            //Ожидание освобождения файла
            while (n != 1)
            {
                if (!IsFileLocker(filePath))
                {
                    while (!CreateLockFile(lockFile))
                    {
                    }
                    n++;
                }
            }
        }

        public static void Unlock(string filePath)
        {
            string lockFile = @"" + new FileInfo(filePath).DirectoryName + "/lock_" + new FileInfo(filePath).Name.Substring(0, 2) + ".txt";
            
            //Удаляем lock-файл
            while (DeleteLockFile(lockFile))
            {
            }
        }

        //Метод, определяющий используется ли в настоящее время файл
        private static bool IsFileLocker(string filePath)
        {
            try
            {
                //проверяем возможность открыть для чтения
                using (FileStream fileOpen = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    fileOpen.Dispose();
                }
                //fileOpen.Close();
                return false;
            }
            catch
            {
                return true;
            }

        }

        private static bool CreateLockFile(string lockFilePath)
        {
            try
            {
                File.Create(lockFilePath);
            }
            catch
            {
                return false;
            }
            return true;
        }

        private static bool DeleteLockFile(string lockFilePath)
        {
            try
            {
                File.Delete(lockFilePath);
            }
            catch
            {
                return false;
            }
            return true;
        }
    }
}
