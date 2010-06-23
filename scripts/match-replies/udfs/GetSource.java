package udfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class GetSource extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        try {
	    // This wouldn't actually work with real tweets, but it does work with
	    // our fake tweets.
	    String tweet = (String)input.get(0);
	    StringTokenizer st = new StringTokenizer(tweet);
	    String at_user = st.nextToken(); // "@barackobama"
	    return at_user.substring(1);
        } catch (ExecException ee) {
            throw ee;
        }
    }

	/*
	 * The API used to propagate schemas is rather confusing and not always crucial,
	 * so I skip it for now.
    @Override
    public Schema outputSchema(Schema input) {
        } catch (FrontendException e) {
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }
	 */
};
