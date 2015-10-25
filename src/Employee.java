
public class Employee {
    private String id;
    private String name;
    private String designation;
    private String address;

    public Employee(String id, String name, String designation, String address) {
        this.id = id;
        this.name = name;
        this.designation = designation;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesignation() {
        return designation;
    }

    public void setDesignation(String designation) {
        this.designation = designation;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
