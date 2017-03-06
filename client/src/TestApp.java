public class TestApp {
    public static void main(String[] args) {

        String peerAccessPoint = args[0];
        String operation = args[1];

        switch (operation){
            case "BACKUP":
                String pathName = args[2];
                int replicationDegree = Integer.parseInt(args[3]);
                break;
            case "RESTORE":
                String pathName = args[2];
                break;
            case "DELETE":
                String pathName = args[2];
                break;
            case "STORAGE":
                int maximumDiskSpace = Integer.parseInt(args[2]);
                break;
            case "STATE":
                break;

        }
    }
}
