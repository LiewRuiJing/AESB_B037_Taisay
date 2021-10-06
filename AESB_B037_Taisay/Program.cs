using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AESB;
using System.Xml;
using System.Data;
using System.IO;
using System.Windows;

namespace AESB_B017_Taisay


{
    class Mai
    {
        public static ConsoleKeyInfo StreamWriter { get; private set; }

        static void Main(string[] args)
        {

            readServerInfor xmldata = new readServerInfor();

            xmldata.readXml(@"C:\Users\Kong Pui Boon\Documents\newxml.xml");
            DBConnection conn = new DBConnection(xmldata.server, xmldata.database, xmldata.username, xmldata.password);
            DataTable dt = conn.GetDataTable("Select * FROM Item");
            int i = 0;
            try
            {
                StreamWriter sw = new StreamWriter(xmldata.path, true);
                foreach (DataRow row in dt.Rows)
                {
                    object[] ary = row.ItemArray;
                    for (i = 0; i < ary.Length - 1; i++)
                    {
                        if (ary[i].ToString() == "")
                        {
                            sw.Write("NULL | ");
                        }
                        else
                        {
                            sw.Write(ary[i].ToString() + " | ");
                        }

                    }
                    sw.WriteLine();
                    sw.WriteLine(ary[i].ToString());
                }
                sw.Close();
            }
            catch
            {
                MessageBox.Show("error");
            }
        }
    }
    class readServerInfor
    {
        public string server;
        public string database;
        public string username;
        public string password;
        public string path;



        public void readXml(string uri)
        {
            XmlTextReader reader = new XmlTextReader(uri);
            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element)
                {
                    if (reader.Name == "Server")
                    {
                        server = reader.ReadElementContentAsString();
                    }
                    if (reader.Name == "Database")
                    {
                        database = reader.ReadElementContentAsString();
                    }
                    if (reader.Name == "Username")
                    {
                        username = reader.ReadElementContentAsString();
                    }
                    if (reader.Name == "Password")
                    {
                        password = reader.ReadElementContentAsString();
                    }
                    if (reader.Name == "Path")
                    {
                        path = reader.ReadElementContentAsString();
                    }
                }
            }
        }

        public void show()
        {
            Console.WriteLine(server + " " + database + " " + username + " " + password + " " + path);
        }

    }



}