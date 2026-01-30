package integration.container.tests.hibernate;
import org.hibernate.jpa.QueryHints;
import javax.persistence.*;

@Entity
@NamedQuery(
    name = "Plane.findAll",
    query = "SELECT p FROM Plane p",
    hints = @QueryHint(name = QueryHints.HINT_COMMENT, value = "+CACHE_PARAM(ttl=250s)")
)
@Table(name = "Plane")
public class Plane {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, unique = true)
  private String name;

  public Plane() {}

  public Plane(String name) {
    this.name = name;
  }

  public Long getId() { return id; }
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
}
