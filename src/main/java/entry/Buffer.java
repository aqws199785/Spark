package entry;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Buffer implements Serializable {
    private Long b1;
    private Long b2;

    public Buffer() {
    }

    public Buffer(Long b1, Long b2) {
        this.b1 = b1;
        this.b2 = b2;
    }

    @Override
    public String toString() {
        return "b1:" + b1 + "\t" +
                "b2:" + b2;
    }
}
