using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergeDataTable
{
    class Program
    {
        static void Main(string[] args)
        {

            DataTable DataTable1 = new DataTable();
            DataTable DataTable2 = new DataTable();
            DataTable DataTable3 = new DataTable();

            DataTable1.Columns.Add("ID");
            DataTable1.Columns.Add("ColA");
            DataTable1.Columns.Add("ColA1");
            DataTable1.Rows.Add(1, "A", "AA");
            DataTable1.Rows.Add(2, "A1", null);
            DataTable1.Rows.Add(3, null, null);

            DataTable2.Columns.Add("ID");
            DataTable2.Columns.Add("ColB");
            DataTable2.Columns.Add("ColB1");
            DataTable2.Rows.Add(1, "B", "BB");
            DataTable2.Rows.Add(3, "B1", "BB1");
            DataTable2.Rows.Add(5, null, "BB2");
            DataTable2.Rows.Add(7, null, null);

            DataTable3.Columns.Add("ID");
            DataTable3.Columns.Add("ColC");
            DataTable3.Columns.Add("ColC1");
            DataTable3.Rows.Add(1, "C", "CC");
            DataTable3.Rows.Add(3, "C1", "CC1");
            DataTable3.Rows.Add(6, null, "CC2");
            DataTable3.Rows.Add(5, null, null);

            Func<DataRow, string> keygen = c => c["ID"].ToString();
            DataTable temp = DataTable1.FullOuterJoin<string>(DataTable2, keygen, keygen).FullOuterJoin<string>(DataTable3, keygen, keygen);

            Console.ReadKey();
        }
    }
    /// <summary>
    /// Helper Extension methods on Datatable 
    /// </summary>
    public static class DataTableHelper
    {

        public static DataTable FullOuterJoin<T>(this DataTable table1, DataTable table2, Func<DataRow, T> Table1keygen, Func<DataRow, T> Table2keygen)
        {
            // perform inital outer join operation
            var outerjoin =
                (
                    from row1 in table1.AsEnumerable()
                    join row2 in table2.AsEnumerable()
                        on Table1keygen(row1) equals Table2keygen(row2)
                        into matches
                    from row2 in matches.DefaultIfEmpty()
                    select new { key = Table1keygen(row1), row1, row2 }
                ).Union(
                    from row2 in table2.AsEnumerable()
                    join row1 in table1.AsEnumerable()
                        on Table2keygen(row2) equals Table1keygen(row1)
                        into matches
                    from row1 in matches.DefaultIfEmpty()
                    select new { key = Table2keygen(row2), row1, row2 }
                );

            // Create result table
            DataTable result = new DataTable();
            result.Columns.Add("ID", typeof(string));
            foreach (var col in table1.Columns.OfType<DataColumn>())
                result.Columns.Add("T1_" + col.ColumnName, col.DataType);
            foreach (var col in table2.Columns.OfType<DataColumn>())
                result.Columns.Add("T2_" + col.ColumnName, col.DataType);

            // fill table from join query
            var row1def = new object[table1.Columns.Count];
            var row2def = new object[table2.Columns.Count];
            foreach (var src in outerjoin)
            {
                // extract values from each row where present
                var data1 = (src.row1 == null ? row1def : src.row1.ItemArray);
                var data2 = (src.row2 == null ? row2def : src.row2.ItemArray);

                // create row with key string and row values
                result.Rows.Add(new object[] { src.key.ToString() }.Concat(data1).Concat(data2).ToArray());
            }

            return result;
        }
    }
}
