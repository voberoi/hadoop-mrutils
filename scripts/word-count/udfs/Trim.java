package udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class Trim extends EvalFunc<Tuple> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        try {
			// Should normally validate input here, but for example's sake...
			String word = (String) input.get(0);
			return mTupleFactory.newTuple(word.trim());
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
