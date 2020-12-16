package NBD_Assignment8;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import java.net.UnknownHostException;

    public class Assignment8 {

        public static class DBObjectUpdate extends UpdateValue.Update<DBObject> {
            private final DBObject update;
            public DBObjectUpdate(DBObject update){
                this.update = update;
            }

            @Override
            public DBObject apply(DBObject t) {
                if(t == null) {
                    t = new DBObject();
                }

                t.firstName = update.firstName;
                t.lastName = update.lastName;
                t.nationality = update.nationality;
                t.profession = update.profession;
                t.birthDate = update.birthDate;
                t.band = update.band;

                return t;
            }
        }

        private static RiakCluster setUpCluster() throws UnknownHostException {
            RiakNode node = new RiakNode.Builder()
                    .withRemoteAddress("127.0.0.1")
                    .withRemotePort(8087)
                    .build();

            RiakCluster cluster = new RiakCluster.Builder(node)
                    .build();

            cluster.start();

            return cluster;
        }

        public static void main( String[] args ) {
            try {
                RiakCluster cluster = setUpCluster();
                RiakClient client = new RiakClient(cluster);
                System.out.println("Client object created");

                //creating document and storing it to Riak
                DBObject entry = new DBObject("Simon", "Posford", "british", "musician", "1971-Oct-28");
                System.out.println("DB object created");

                Namespace bucket = new Namespace("22362");
                Location location = new Location(bucket, entry.lastName);
                StoreValue storeObject = new StoreValue.Builder(entry)
                        .withLocation(location)
                        .build();
                client.execute(storeObject);
                System.out.println("Object " + entry.firstName + " " + entry.lastName + " is now stored in Riak");

                //fetching document from Riak
                FetchValue fetchObject = new FetchValue.Builder(location)
                        .build();
                DBObject val = client.execute(fetchObject).getValue(DBObject.class);
                System.out.println("Object " + val.firstName + " " + val.lastName + " successfully fetched from Riak");

                //outputting fetched document
                String s = String.format("Name: %s, nationality: %s, profession: %s, birth date: %s, band: %s", val.firstName+" "+val.lastName, val.nationality, val.profession, val.birthDate, val.band);
                System.out.println(s);

                //modifying document in Riak
                entry.band = "Shpongle";
                DBObjectUpdate updatedEntry = new DBObjectUpdate(entry);
                UpdateValue updateValue = new UpdateValue.Builder(location)
                        .withUpdate(updatedEntry).build();
                UpdateValue.Response response = client.execute(updateValue);
                System.out.println("Object " + val.firstName + " " + val.lastName + " updated");

                //fetching document from Riak
                fetchObject = new FetchValue.Builder(location)
                        .build();
                val = client.execute(fetchObject).getValue(DBObject.class);
                System.out.println("Object " + val.firstName + " " + val.lastName + " successfully fetched from Riak");

                //outputting fetched document
                s = String.format("Name: %s, nationality: %s, profession: %s, birth date: %s, band: %s", val.firstName+" "+val.lastName, val.nationality, val.profession, val.birthDate, val.band);
                System.out.println(s);

                //deleting document from Riak
                DeleteValue deleteDoc = new DeleteValue.Builder(location)
                        .build();
                client.execute(deleteDoc);
                System.out.println("Document deleted");

                //attempting to fetch and output deleted document
                fetchObject = new FetchValue.Builder(location)
                        .build();
                if(!fetchObject.equals(null)) {
                    val = client.execute(fetchObject).getValue(DBObject.class);
                    System.out.println("Object " + val.firstName + " " + val.lastName + " successfully fetched from Riak");
                }
                System.out.println("Document not found");

                cluster.shutdown();

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        public static class DBObject{
            public String firstName;
            public String lastName;
            public String nationality;
            public String profession;
            public String birthDate;
            public String band;

            public DBObject() {
                firstName = "";
                lastName = "";
                nationality = "";
                profession = "";
                birthDate = "";
                band = "";
            }

            public DBObject(String firstName, String lastName, String nationality, String profession, String birthDate) {
                this.firstName = firstName;
                this.lastName = lastName;
                this.nationality = nationality;
                this.profession = profession;
                this.birthDate = birthDate;
                band = "N/A";
            }

            public DBObject(String firstName, String lastName, String nationality, String profession, String birthDate, String band) {
                this.firstName = firstName;
                this.lastName = lastName;
                this.nationality = nationality;
                this.profession = profession;
                this.birthDate = birthDate;
                this.band = band;
            }
        }
    }
