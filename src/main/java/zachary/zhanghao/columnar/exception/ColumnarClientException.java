/**
 * 
 */
package zachary.zhanghao.columnar.exception;

/**
 * 框架向外面抛出的check异常
 * 
 * @author zachary.zhang
 *
 */
public class ColumnarClientException extends Exception {

    private static final long serialVersionUID = 5323133031165289721L;

    public ColumnarClientException(String message) {
        super(message);
    }

    public ColumnarClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ColumnarClientException(Throwable cause) {
        super(cause);
    }
}
