package simpledb;

/** Predicate compares tuples to a specified Field value.
 */
public class Predicate {

    /** Constants used for return codes in Field.compare */
    public enum Op {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(int s) {
            return values()[s];
        }

        public String toString() {
            switch (this) {
            case EQUALS:
                return "=";
            case GREATER_THAN:
                return ">";
            case LESS_THAN:
                return "<";
            case LESS_THAN_OR_EQ:
                return "<=";
            case GREATER_THAN_OR_EQ:
                return ">=";
            case LIKE:
                return "LIKE";
            default:
                return "";
            }
        }
    }

    private final int field;
    private final Op op;
    private final Field operand;

    /** Constructor -- create a new predicate.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field (constant value) to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /** Return the field number for the field to be compared */
    public int getField() {
        return field;
    }

    /** Return the operator */
    public Op getOp() {
        return op;
    }

    /** Return the operand against which the field is to be compared */
    public Field getOperand() {
        return operand;
    }

    /** Return true if the tuple t satisfies the predicate. */
    public boolean filter(Tuple t) {
        Field f = t.getField(field);
        return f.compare(op, operand);
    }

    /**
     * Returns something useful, like
     * "f = field_id op = op_string operand = operand_string
     */
    public String toString() {
        return String.format("f = %d op = %s operand = %s", field, op.toString(),
                             operand == null ? "null" : operand.toString());
    }
}
