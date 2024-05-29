package entry;

import lombok.Getter;
import lombok.Setter;
import java.io.Serializable;

@Getter
@Setter
public class HotCategory implements Serializable {
    private String id;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public HotCategory() {
    }

    public HotCategory(String id, Long clickCount, Long orderCount, Long payCount) {
        this.id = id;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        String str = "id:" + id + "\t" +
                "clickCount:" + clickCount + "\t" +
                "orderCount:" + orderCount + "\t" +
                "payCount:" + payCount;
        return str;
    }
}
