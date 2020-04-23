/*
 * 1. setup your hbase client properly (make hbase shell work from console)
 *    setup ZooKeeper addr in hbase-site.xml
 * 2. Launch via:
 *    $ CLASSPATH="`hbase classpath`:out/artifacts/HBaseExamples/HBaseExamples.jar" java HBaseRandomAccess scan 2>/dev/null
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class HBaseRandomAccess {
    private String table_name = null;

    HBaseRandomAccess(String table) {
        table_name = table;
    }

    void DemoPut() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(table_name));
        Put put = new Put(Bytes.toBytes("microsoft.com"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("owner"), Bytes.toBytes("Bill Gates"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_updated"), Bytes.toBytes(1478039132000L));
        table.put(put);


        table.close();
        connection.close();
    }

    void DemoGet() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(Bytes.toBytes("microsoft.com"));
        Result res = table.get(get);
        for(Cell cell: res.listCells()) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.printf("Qualifier: %s : Value: %s\n", qualifier, value);
        }

        table.close();
        connection.close();
    }

    void PrintCell(Cell cell) {
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
        String value = Bytes.toString(CellUtil.cloneValue(cell));
        System.out.printf("\t%s=%s\n", qualifier, value);
    }

    void PrintResult(Result res) throws UnsupportedEncodingException {
        System.out.printf("------------- ROW: %s\n", new String(res.getRow(), "UTF8"));
        for (Cell cell: res.listCells())
            PrintCell(cell);
    }

    void DemoScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan().addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("text"));
        ResultScanner scanner = table.getScanner(scan);

        for (Result res: scanner)
            PrintResult(res);

        table.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            usage();
        
        HBaseRandomAccess demo = new HBaseRandomAccess(args[0]);
        String action = args[1];

        if (action.equals("put"))
            demo.DemoPut();
        else if (action.equals("get"))
            demo.DemoGet();
        else if (action.equals("scan"))
            demo.DemoScan();
        else
            usage();
    }

    static void usage() {
        System.err.println("Usage: jar table {put|get|scan}");
        System.exit(64);
    }
}
