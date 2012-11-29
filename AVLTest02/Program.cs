using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Net;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

namespace AVLTest02
{
    class Program
    {
        static string SERVER_BASE = "http://ddot-updates.herokuapp.com";
        static string ADHERENCE_ENDPOINT;
        static string TRIPS_ENDPOINT;
        static string STOPS_ENDPOINT;
        static string BLOCKS_ENDPOINT;

        static int lastShortId = 0;
        static int lastRegularId = 0;
        enum AdherenceType
        {
            Regular,
            Short
        }

        static string GetAdherenceData(SqlConnection connection, AdherenceType type)
        {
            //string queryString = "SELECT TOP 10 *  FROM dbo.TIME_POINT_CROSSING;";
            //string queryString = "SELECT T.ROUTE_ID, T.TRIP_ID, (T.ACT_ARRIVAL_TIME - T.SCHEDULED_TIME) AS Adherence, " +
            //string queryString = "SELECT T.ROUTE_ID, T.TRIP_ID, (T.ACT_DEPARTURE_TIME - T.SCHEDULED_TIME) AS Adherence, " +
            //"T.GEO_NODE_ID, T.TIME_POINT_ID FROM dbo.TIME_POINT_CROSSING AS T " +
            //    "JOIN (SELECT TREF.TRIP_ID, MAX(TREF.ACT_ARRIVAL_TIME) AS MAX_ACT_ARRIVAL_TIME FROM dbo.TIME_POINT_CROSSING AS TREF " +
            //    "WHERE TREF.CALENDAR_ID=@today GROUP BY TREF.TRIP_ID) AS AT " +
            //    "ON (T.TRIP_ID = AT.TRIP_ID AND T.ACT_ARRIVAL_TIME = AT.MAX_ACT_ARRIVAL_TIME) " +
            //    "WHERE T.CALENDAR_ID=@today AND T.TRIP_ID IS NOT NULL;";

            string queryString;

            if (type == AdherenceType.Short)
            {
                queryString = "SELECT TOP 500 L.LOGGED_MESSAGE_SHORT_ID, L.ADHERENCE, W.WORK_PIECE_ID, L.MESSAGE_TIMESTAMP " +
                "FROM TMDailyLog.dbo.LOGGED_MESSAGE_SHORT AS L " +
                "JOIN TMMain.dbo.VEHICLE AS V " +
                "ON L.SOURCE_HOST=V.RNET_ADDRESS " +
                "JOIN (SELECT * FROM TMDailyLog.dbo.DAILY_WORK_PIECE AS DWP " +
                "WHERE DWP.CALENDAR_ID=@today " +
                "AND DWP.SCHEDULED_LOGON_TIME < @seconds " +
                "AND DWP.SCHEDULED_LOGOFF_TIME > @seconds) AS W " +
                "ON W.CURRENT_VEHICLE_ID=V.VEHICLE_ID " +
                "WHERE L.LOGGED_MESSAGE_SHORT_ID>@lastId " +
                "AND L.CALENDAR_ID=@today " +
                "ORDER BY L.LOGGED_MESSAGE_SHORT_ID DESC;";
            }
            else
            {
                queryString = "SELECT TOP 500 L.TRANSMITTED_MESSAGE_ID, L.ADHERENCE, W.WORK_PIECE_ID, L.MESSAGE_TIMESTAMP " +
                "FROM TMDailyLog.dbo.LOGGED_MESSAGE AS L " +
                "JOIN TMMain.dbo.VEHICLE AS V " +
                "ON L.SOURCE_HOST=V.RNET_ADDRESS " +
                "JOIN (SELECT * FROM TMDailyLog.dbo.DAILY_WORK_PIECE AS DWP " +
                "WHERE DWP.CALENDAR_ID=@today " +
                "AND DWP.SCHEDULED_LOGON_TIME < @seconds " +
                "AND DWP.SCHEDULED_LOGOFF_TIME > @seconds) AS W " +
                "ON W.CURRENT_VEHICLE_ID=V.VEHICLE_ID " +
                "WHERE L.TRANSMITTED_MESSAGE_ID>@lastId " +
                "AND L.CALENDAR_ID=@today " +
                "AND L.ADHERENCE IS NOT NULL " +
                "ORDER BY L.TRANSMITTED_MESSAGE_ID DESC;";
            }

            SqlCommand command = new SqlCommand(queryString, connection);

            // TODO: make sure we operate properly in Detroit time, considering
            // daylight savings.
            DateTime today = DateTime.Today;
            // The calendar ID should have the form 120120612
            int todayNum = 100000000 + (today.Year * 10000) + (today.Month * 100) + today.Day;
            command.Parameters.Add(new SqlParameter("@today", todayNum));

            // TODO: make sure we operate properly in Detroit time, considering
            // daylight savings and "transit time", which would count 1am as
            // part of the previous day.
            var now = DateTime.UtcNow.AddHours(-4);
            var seconds = (now.Hour * 3600) + (now.Minute * 60) + now.Second;
            command.Parameters.Add(new SqlParameter("@seconds", seconds));

            if (type == AdherenceType.Short)
            {
                command.Parameters.Add(new SqlParameter("@lastId", lastShortId));
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@lastId", lastRegularId));
            }

            SqlDataReader reader = command.ExecuteReader();

            string data = String.Empty;

            var readId = false;
            try
            {
                StringBuilder sb = new StringBuilder();
                while (reader.Read())
                {
                    if (!readId)
                    {
                        int? id = reader[0] as int?;
                        if (id.HasValue)
                        {
                            if (type == AdherenceType.Short)
                            {
                                lastShortId = id.Value;
                            }
                            else
                            {
                                lastRegularId = id.Value;
                            }
                        }
                        readId = true;
                    }
                    sb.Append(String.Format("{0},{1},{2},{3}\n", reader[0], reader[1], reader[2], reader[3]));
                    Console.WriteLine(String.Format("{0}, {1}, {2}, {3}", reader[0], reader[1], reader[2], reader[3]));
                }
                data = sb.ToString();
            }
            finally
            {
                reader.Close();
            }

            return data;
        }

        static string GetTripData(SqlConnection connection)
        {
            string queryString = "SELECT T.TRIP_ID, T.TRIP_END_TIME, " +
                "(SELECT GEO_NODE_NAME FROM dbo.GEO_NODE " +
                "WHERE T.TRIP_END_NODE_ID = dbo.GEO_NODE.GEO_NODE_ID) AS EndNode, " +
                "(SELECT G.GEO_NODE_NAME FROM dbo.GEO_NODE AS G " +
                "WHERE T.TRIP_START_NODE_ID = G.GEO_NODE_ID) AS StartNode, " +
                "T.BLOCK_ID " +
                "FROM dbo.TRIP AS T WHERE TIME_TABLE_VERSION_ID=77;";

            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            string data = String.Empty;
            int count = 0;

            try
            {
                StringBuilder sb = new StringBuilder();
                while (reader.Read())
                {
                    sb.Append(String.Format("{0},{1},{2},{3},{4}\n", reader[0], reader[1], reader[2], reader[3], reader[4]));
                    //Console.WriteLine(String.Format("{0}, {1}, {2}, {3}, {4}", reader[0], reader[1], reader[2], reader[3], reader[4]));
                    count += 1;
                }
                data = sb.ToString();
                Console.WriteLine("Send {0} lines of AVL trip data.", count);
            }
            finally
            {
                reader.Close();
            }

            return data;
        }

        static string GetStopData(SqlConnection connection)
        {
            string queryString = "SELECT GEO_NODE_ID, GEO_NODE_NAME FROM dbo.GEO_NODE;";

            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            string data = String.Empty;
            int count = 0;

            try
            {
                StringBuilder sb = new StringBuilder();
                while (reader.Read())
                {
                    sb.Append(String.Format("{0}, {1}\n", reader[0], reader[1]));
                    //Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                    count += 1;
                }
                data = sb.ToString();
                Console.WriteLine("Send {0} lines of AVL stop data.", count);
            }
            finally
            {
                reader.Close();
            }

            return data;
        }

        static string GetBlockData(SqlConnection connection)
        {
            string queryString = "SELECT WP.WORK_PIECE_ID, WP.BLOCK_ID " +
                "FROM TMMain.dbo.WORK_PIECE AS WP " +
                "WHERE TIME_TABLE_VERSION_ID=77;";

            SqlCommand command = new SqlCommand(queryString, connection);
            SqlDataReader reader = command.ExecuteReader();

            string data = String.Empty;
            int count = 0;

            try
            {
                StringBuilder sb = new StringBuilder();
                while (reader.Read())
                {
                    sb.Append(String.Format("{0},{1}\n", reader[0], reader[1]));
                    //Console.WriteLine(String.Format("{0}, {1}", reader[0], reader[1]));
                    count += 1;
                }
                data = sb.ToString();
                Console.WriteLine("Send {0} lines of AVL work piece/block data.", count);
            }
            finally
            {
                reader.Close();
            }

            return data;
        }

        static string GetResponseString(HttpWebResponse resp)
        {
            Stream responseStream = resp.GetResponseStream();
            Encoding encoding = new UTF8Encoding();
            string enc = resp.CharacterSet;
            if (enc != null)
            {
                encoding = Encoding.GetEncoding(enc);
            }

            StringBuilder sb = new StringBuilder();
            int bufSize = 256;
            byte[] buf = new byte[256];
            int count = responseStream.Read(buf, 0, bufSize);
            while (count > 0)
            {
                sb.Append(encoding.GetString(buf, 0, count));
                count = responseStream.Read(buf, 0, bufSize);
            }
            return sb.ToString();
        }

        static string PostPlaintext(string endpoint, string data)
        {
            HttpWebRequest request = HttpWebRequest.Create(endpoint) as HttpWebRequest;
            if (request == null)
            {
                Console.WriteLine("Problem with the endpoint URL: {0}", ADHERENCE_ENDPOINT);
                return String.Empty;
            }
            request.Method = "POST";
            request.ContentType = "text/plain";

            var encoding = new UTF8Encoding();
            byte[] bytes = encoding.GetBytes(data);
            request.ContentLength = bytes.Length;
            string responseString = String.Empty;
            try
            {
                Stream requestStream = request.GetRequestStream();
                requestStream.Write(bytes, 0, bytes.Length);

                Console.WriteLine("request.ContentLength = {0}", request.ContentLength);

                try
                {
                    using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                    {
                        responseString = GetResponseString(response);
                        Console.WriteLine(responseString);
                    }
                }
                catch (WebException e)
                {
                    Console.WriteLine("Got WebException: {0}", e);
                }
                requestStream.Close();
            }
            catch (WebException e)
            {
                Console.WriteLine("Got WebException getting request stream: {0}", e);
                Console.WriteLine(e.Message);
            }
            return responseString;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adherenceData"></param>
        /// <returns>true if we need to post static AVL data, false otherwise</returns>
        static bool PostAdherenceData(string adherenceData)
        {
            var rx = new Regex(@"""needsStaticData""\w*:\w*true");

            string responseString = PostPlaintext(ADHERENCE_ENDPOINT, adherenceData);
            Console.WriteLine("Response from adherence post:\n{0}", responseString);
            if (rx.IsMatch(responseString))
            {
                Console.WriteLine("Needs static AVL data.");
                return true;
            }
            return false;
        }

        static bool GetAndPostAdherence(AdherenceType type)
        {
            bool needsStaticData = false;
            string realtimeConnectionString = "Data Source=10.63.3.6;Initial Catalog=TMDailyLog;Connection Timeout=15;Integrated Security=false;User ID=CfA;Password=CfA2012";
            using (SqlConnection connection = new SqlConnection(realtimeConnectionString))
            {
                try
                {
                    connection.Open();
                    String adherenceData = GetAdherenceData(connection, type);
                    needsStaticData = PostAdherenceData(adherenceData);
                }
                catch (SqlException e)
                {
                    Console.WriteLine(e.Message);
                }
            }

            return needsStaticData;
        }

        static void GetAndPostStatic()
        {
            string staticConnectionString = "Data Source=10.63.3.6;Initial Catalog=TMMain;Connection Timeout=15;Integrated Security=false;User ID=CfA;Password=CfA2012";
            using (SqlConnection connection = new SqlConnection(staticConnectionString))
            {
                try
                {
                    connection.Open();
                    string tripData = GetTripData(connection);
                    string stopData = GetStopData(connection);
                    string blockData = GetBlockData(connection);
                    PostPlaintext(TRIPS_ENDPOINT, tripData);
                    PostPlaintext(STOPS_ENDPOINT, stopData);
                    PostPlaintext(BLOCKS_ENDPOINT, blockData);
                }
                catch (SqlException e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                SERVER_BASE = args[0];
            }
            ADHERENCE_ENDPOINT = SERVER_BASE + "/adherence";
            TRIPS_ENDPOINT = SERVER_BASE + "/static-avl/trips";
            STOPS_ENDPOINT = SERVER_BASE + "/static-avl/stops";
            BLOCKS_ENDPOINT = SERVER_BASE + "/static-avl/blocks";
            while (true)
            {
                bool needsStaticData = GetAndPostAdherence(AdherenceType.Short);

                if (needsStaticData)
                {
                    GetAndPostStatic();
                    // Repost adherence now, since the server was unable to use the last set of data.
                    // TODO: In production, we can potentially just wait until the next cycle.
                    GetAndPostAdherence(AdherenceType.Short);
                    GetAndPostAdherence(AdherenceType.Regular);
                }
                else
                {
                    GetAndPostAdherence(AdherenceType.Regular);
                }

                Thread.Sleep(60000);
            }
        }
    }
}
