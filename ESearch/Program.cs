﻿using PlainElastic.Net;
using PlainElastic.Net.Queries;
using PlainElastic.Net.Serialization;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ESearch
{
    class Program
    {
        static void Main(string[] args)
        {
            ElasticConnection client = new ElasticConnection("localhost", 9200);
            SearchCommand cmd = new SearchCommand("movie", "items");
            var query = new QueryBuilder<VerycdItem>()
            .Query(b =>
                    b.Bool(m =>
                    //并且关系
                    m.Must(t =>
                        t.QueryString(t1 => t1.DefaultField("content").Query("成龙")).QueryString(t2 => t2.DefaultField("title").Query("龙"))
                            )
                        )
                    ).Size(100)
            .Build();

            //DeleteCommand delCmd = new DeleteCommand()
            var result = client.Post(cmd, query);
            var serializer = new JsonNetSerializer();
            var searchResult = serializer.ToSearchResult<VerycdItem>(result);
            //searchResult.hits.total; //一共有多少匹配结果
            // searchResult.Documents;//当前页的查询结果 
            foreach (var doc in searchResult.Documents)
            {
                Console.WriteLine(doc.title);
            }
            Console.ReadKey();
        }

        static void Main1(string[] args)
        {

            using (SQLiteConnection conn = new SQLiteConnection(@"Data Source=G:\其他资料\技术资料\各种海量数据\VeryCD\verycd.sqlite3.db"))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select * from verycd";
                    using (var reader = cmd.ExecuteReader())
                    {
                        ElasticConnection client = new ElasticConnection("localhost", 9200);
                        var serializer = new JsonNetSerializer();
                        while (reader.Read())
                        {
                            long verycdid = reader.GetInt64(reader.GetOrdinal("verycdid"));
                            string title = reader.GetString(reader.GetOrdinal("title"));
                            string status = reader.GetString(reader.GetOrdinal("status"));
                            string brief = reader.GetString(reader.GetOrdinal("brief"));
                            string pubtime = reader.GetString(reader.GetOrdinal("pubtime"));
                            string updtime = reader.GetString(reader.GetOrdinal("updtime"));
                            string category1 = reader.GetString(reader.GetOrdinal("category1"));
                            string category2 = reader.GetString(reader.GetOrdinal("category2"));
                            string ed2k = reader.GetString(reader.GetOrdinal("ed2k"));
                            string content = reader.GetString(reader.GetOrdinal("content"));
                            string related = reader.GetString(reader.GetOrdinal("related"));

                            VerycdItem item = new VerycdItem();
                            item.verycdid = verycdid;
                            item.title = title;
                            item.status = status;
                            item.brief = brief;
                            item.pubtime = pubtime;
                            item.updtime = updtime;
                            item.category1 = category1;
                            item.category2 = category2;
                            item.ed2k = ed2k;
                            item.content = content;
                            item.related = related;

                            Console.WriteLine("当前读取到id=" + verycdid);
                            IndexCommand indexCmd = new IndexCommand("verycd", "items", verycdid.ToString());
                            //Put()第二个参数是要插入的数据
                            OperationResult result = client.Put(indexCmd, serializer.Serialize(item));
                            var indexResult = serializer.ToIndexResult(result.Result);
                            if (indexResult.created)
                            {
                                Console.WriteLine("创建了");
                            }
                            else
                            {
                                Console.WriteLine("没创建" + indexResult.error);
                            }
                        }
                    }
                }
            }



            Console.ReadKey();
        }
        
    }
}
