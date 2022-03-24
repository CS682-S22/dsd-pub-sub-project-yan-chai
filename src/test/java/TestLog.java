import Broker.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestLog {
    static final Logger logger = LogManager.getLogger();

    @Test
    public void test() {

        logger.info("test!test!");
        logger.warn("????????hello?");
        logger.error("I am a logger!");
        Assert.assertTrue(logger.isErrorEnabled());
        Assert.assertTrue(logger.isInfoEnabled());
        Assert.assertTrue(logger.isWarnEnabled());
    }
}
