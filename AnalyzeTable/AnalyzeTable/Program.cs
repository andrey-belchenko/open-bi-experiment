using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace AnalyzeTable
{
    class Program
    {
        static void Main(string[] args)
        {
            string queryString = "SELECT *  FROM  [stg].[Project]";
            string connectionString = "User ID=conteq;Password=conteq;Initial Catalog=Ilim.StagingArea;Server=192.168.0.115";
            var dt = new DataTable();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(queryString, connection);
                connection.Open();
                // SqlDataReader reader = command.ExecuteReader();
                var da = new SqlDataAdapter();
                da.SelectCommand = command;

                da.Fill(dt);
                dt.Columns.Add("XID", typeof(int));
                int i = 1;
                foreach (DataRow r in dt.AsEnumerable())
                {
                    r["XID"] = i;
                    i++;
                }
            }
            //var dc1 = new Dictionary<string, int>();
            var dti1 = new DataTable();
            dti1.Columns.Add("column");
            dti1.Columns.Add("count", typeof(int));
            foreach (DataColumn col in dt.Columns)
            {
                var cnt = dt.AsEnumerable().Select(r => r[col.ColumnName]).Distinct().Count();
                dti1.Rows.Add(col.ColumnName, cnt);
            }
            var dti2 = new DataTable();
            dti2.Columns.Add("column");
            dti2.Columns.Add("count");
            dti1.AsEnumerable().OrderBy(r => r["count"]);
            foreach (DataRow r in dti1.AsEnumerable().OrderBy(r => r["count"]))
            {
                dti2.Rows.Add(r["column"], r["count"]);
            }

            var dti3 = new DataTable();
            dti3.Columns.Add("left");
            dti3.Columns.Add("right");
            dti3.Columns.Add("relation", typeof(int));
            dti3.Columns.Add("level", typeof(int));
            dti3.Columns.Add("num", typeof(int));
            var processedPairs = new HashSet<string>();
            for (int i1 = 0; i1 < dti2.Rows.Count - 1; i1++)
            {
                var parents = new List<string>();
                AnalyzeColumnRelationship(dti2, i1, dt, processedPairs, parents, 1, dti3);
            }
            var dti4 = new DataTable();
            dti4.Columns.Add("title");
            var allLeft = dti3.AsEnumerable().Select(r => r["left"]).Distinct().ToHashSet();
            var root = dti3.AsEnumerable().Where(r => !allLeft.Contains(r["right"])).Select(r=> r["right"]).First();
            WriteAndGetDescedants(dti3, root, 0, dti4);
            //var sib = new List<object>();
            //CollectSiblings(dti3, root, sib);
            //var title = string.Join(',',sib);
     



            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                // make sure to enable triggers
                // more on triggers in next post
                SqlBulkCopy bulkCopy = new SqlBulkCopy(
                    connection
                    );

                bulkCopy.DestinationTableName = "dbo.Tmp1";
                connection.Open();
                SqlCommand command = new SqlCommand(@"
drop table if exists dbo.Tmp1; 
create table dbo.Tmp1([left] nvarchar(4000) ,[right] nvarchar(4000) , [relation] int, [level] int, [num] int);   
"
                , connection);
                command.ExecuteNonQuery();
                bulkCopy.WriteToServer(dti3);
                connection.Close();
            }
        }

        private static void CollectSiblings(DataTable columnsRelationsInfo,object prev, List<object> collected)
        {
            collected.Add(prev);
            var nextItems = columnsRelationsInfo.AsEnumerable().Where(r => r["right"] == prev && ((int)r["relation"])==1).Select(r => r["left"]).ToArray();
            foreach(var item in nextItems)
            {
                CollectSiblings(columnsRelationsInfo, item, collected);
            }
        }

        private static void WriteAndGetDescedants(DataTable columnsRelationsInfo, object item, int level, DataTable result)
        {
            var siblings = new List<object>();
            CollectSiblings(columnsRelationsInfo, item, siblings);
            var title = new string('\t', level) + string.Join(',', siblings);
            result.Rows.Add(title);
            var children = columnsRelationsInfo.AsEnumerable().Where(r => siblings.Contains(r["right"]) && ((int)r["relation"]) == 2).Select(r => r["left"]).Distinct().ToArray();
            foreach (var child in children)
            {
                WriteAndGetDescedants(columnsRelationsInfo, child, level + 1, result);
              
            }
        }

        private static void AnalyzeColumnRelationship(DataTable columnsInfo, int columnIndex,DataTable data,HashSet<string> processedPairs, IEnumerable<string> parents, int level, DataTable results)
        {
            var dti2 = columnsInfo;
            var dti3 = results;
            var i1 = columnIndex;
            var col1Name = dti2.Rows[i1]["column"].ToString();
            var count1 = dti2.Rows[i1]["count"].ToString();
           
            for (int i2 = i1 + 1; i2 < dti2.Rows.Count; i2++)
            {
                var relation = 1;
                var col2Name = dti2.Rows[i2]["column"].ToString();
                var count2 = dti2.Rows[i2]["count"].ToString();
                var key = col1Name + "#" + col2Name;
                if (processedPairs.Contains(key))
                {
                    continue;
                }
                processedPairs.Add(key);

                foreach (var parent in parents)
                {
                    var pkey = parent + "#" + col2Name;
                    processedPairs.Add(pkey);
                }
                var dict = new Dictionary<string, HashSet<string>>();
                foreach (var r in data.AsEnumerable())
                {
                    var v2 = r[col1Name].ToString();
                    var v1 = r[col2Name].ToString();
                    
                    if (!dict.ContainsKey(v1))
                    {
                        dict.Add(v1, new HashSet<string>());
                    }
                    if (!dict[v1].Contains(v2))
                    {
                        dict[v1].Add(v2);
                    }
                    if (dict[v1].Count > 1)
                    {
                        relation = 0;
                        break;
                    }
                }
                if (relation == 1)
                {
                    if (count1 != count2)
                    {
                        relation = 2;
                    }
                    var num = dti3.Rows.Count + 1;
                    dti3.Rows.Add(col1Name, col2Name, relation,level,num) ;
                    var nextParents = parents.ToList();
                    nextParents.Add(col1Name);
                    AnalyzeColumnRelationship(columnsInfo, i2, data, processedPairs, nextParents, level+1, results);
                }

                

            }
        }
    }
}
