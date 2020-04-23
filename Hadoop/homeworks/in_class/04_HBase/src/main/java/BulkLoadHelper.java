import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;


class BulkLoadHelper {
    private static Log LOG = LogFactory.getLog(BulkLoadHelper.class);
    private Path tmp_dir = null;
    private Configuration conf = null;

    BulkLoadHelper(Configuration conf) throws IOException {
        this.conf = conf;
        tmp_dir = CreateTempDirectory(new Path("/tmp"));
    }

    BulkLoadHelper(Configuration conf, Path parent) throws IOException {
        this.conf = conf;
        tmp_dir = CreateTempDirectory(parent);
    }

    final Path GetBulksDirectory() {
        return tmp_dir;
    }

    private Path CreateTempDirectory(Path parent) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(parent)) {
            fs.mkdirs(parent);
        }

        String fmt = "/bulk_tmp_%10d-%x";
        Date date = new Date();
        long seconds = date.getTime() / 1000;
        int cnt = 10;
        Random rnd = new Random();

        while (cnt-- >= 0) {
            Path dir = parent.suffix(String.format(fmt, seconds, rnd.nextInt()));
            if (!fs.exists(dir)) {
                return dir;
            }
        }

        throw new IOException("Unbelievable: all dirs for bulk upload are busy!");
    }

    private void Remove() throws IOException {
        FileSystem.get(conf).delete(tmp_dir, true);
    }

    private void MakeAllSubdirsWritable() throws IOException {
        HashSet<Path> subdirs = new HashSet<>();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tmp_dir, true /* recursive */);

        while (iterator.hasNext()) {
            LocatedFileStatus st = iterator.next();
            Path parent = st.getPath().getParent();
            if (parent != tmp_dir)
                subdirs.add(parent);
        }

        for (Path dir: subdirs) {
            FsPermission perm = fs.getFileStatus(dir).getPermission();
            FsPermission new_perm = new FsPermission(perm.getUserAction(), perm.getGroupAction(),
                    FsAction.ALL, perm.getStickyBit());

            if (!new_perm.equals(perm))
                fs.setPermission(dir, new_perm);
        }
    }

    public void BulkLoad(TableName output_table) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(output_table);
        BulkLoad(connection, table);
    }

    public void BulkLoad(Connection connection, Table table) throws Exception {
        LOG.info("Loading hfiles from temp directory " + tmp_dir.toString());
        MakeAllSubdirsWritable();

        try {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(
                    tmp_dir,
                    connection.getAdmin(),
                    table,
                    connection.getRegionLocator(table.getName())
            );
        } finally {
            try {
                LOG.info("Removing temp directory " + tmp_dir.toString());
                Remove();
            } catch (Exception e) {
                LOG.error("Failed to remove temp directory " + tmp_dir.toString());
            }
        }
    }
}
