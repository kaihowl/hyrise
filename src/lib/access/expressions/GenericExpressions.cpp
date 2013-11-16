
#include "GenericExpressions.h"

#include "access/expressions/ExpressionRegistration.h"

namespace hyrise { namespace access {

REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_INT_AND_F2_EQ_INT_AND_F3_EQ_INT);
REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_INT);
REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_INT_AND_F2_EQ_INT);
REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_STRING);
REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_STRING_OR_F2_NEQ_FLOAT);
REGISTER_EXPRESSION_CLASS(Store_FLV_F1_EQ_INT_AND_F2_EQ_INT_AND_F3_EQ_INT_AND_F4_GTE_INT_AND_F5_LTE_INT);
REGISTER_EXPRESSION_CLASS(FLOAT_BTW);
REGISTER_EXPRESSION_CLASS(INT_BTW);
}}
