import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	static Configuration config = HBaseConfiguration.create();
	static Date date = new Date();
	static long time = date.getTime();
	static Timestamp ts = new Timestamp(time);

	public static void main(String[] args)
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {

		config.clear();
		config.set("hbase.zookeeper.quorum", "192.168.159.136");
		HBaseAdmin admin = new HBaseAdmin(config);

		// createTable(admin);
		// putTable(admin);

		// getTable(admin);

		// run("1_KRTEW6PFK8BFNKFH");

	}


	static void run(String id)
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {
		config.clear();
		config.set("hbase.zookeeper.quorum", "192.168.159.136");
		MysqlData.report("Connecting to Hbase.. ");
		HBaseAdmin admin = new HBaseAdmin(config);
		getTable(admin, id);
	}

	private static void getTable(HBaseAdmin admin, String pId)
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {
		// private static void getTable(HBaseAdmin admin) throws IOException {
		HTable hTable = new HTable(config, Bytes.toBytes("vendor_products"));
		// byte[] id = Bytes.toBytes("1_KRTEW6PFK8BFNKFH");
		pId = "1_" + pId;
		byte[] id = Bytes.toBytes(pId);
		Get g = new Get(id);
		Result result = hTable.get(g);
		MysqlData.report("Getting data from Hbase");
		byte[] name = result.getValue(Bytes.toBytes("cache"), Bytes.toBytes("products"));
		File file1 = new File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\json\\sample\\input.json");
		FileUtils.writeByteArrayToFile(file1, name);
		System.out.println("name: " + Bytes.toString(name));

		// JoltTest joltTestObj = new JoltTest();
		// MysqlData.report("Calling method of Jolt class | " + ts);
		// joltTestObj.transform();
	}

}
