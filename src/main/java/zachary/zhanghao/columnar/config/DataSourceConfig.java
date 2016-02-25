/**
 * 
 */
package zachary.zhanghao.columnar.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Properties;

import zachary.zhanghao.columnar.exception.ColumnarClientException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zachary.zhang
 *
 */
@Slf4j
public class DataSourceConfig {

    private String configFile;
    private Properties properties;

    public DataSourceConfig(String configFile) throws ColumnarClientException {
        this.configFile = configFile;

        // 路径中包含中文的需要处理
        String temp = this.getClass().getResource("/").getPath() + this.configFile;
        try (InputStream inputStream = new FileInputStream(URLDecoder.decode(temp, "utf-8"))) {

            Properties properties = new Properties();
            properties.load(inputStream);
            this.properties = properties;
        } catch (IOException e) {
            log.error("read properties file failed,", e);
            throw new ColumnarClientException(e);
        }
    }


    public Properties getProperties() {
        return this.properties;
    }
}
