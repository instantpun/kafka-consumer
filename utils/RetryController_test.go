package utils

import (
	"fmt"
	"testing"
	// "github.com/instantpun/kafka-consumer/utils"
)

type ControllerIncrementTest struct{
	Descriptor       string
	InputController  *RetryConfig
	ExpectedCounter	 uint
}

var (
	NullController_00 = &RetryConfig{}
	LinearController_00, _ = NewRetryConfig("linear")
	ExpController_00, _ = NewRetryConfig("exponential")
	ExpController_01, _ = NewRetryConfig("exponential")
)

var CtlIncrementTests = []ControllerIncrementTest{
	{fmt.Sprintf("RetryConfig%v", NullController_00),   NullController_00,   1},
	// {fmt.Sprintf("RetryConfig%v", NullController_01),   NullController_01,   1},
	{fmt.Sprintf("RetryConfig%v", LinearController_00), LinearController_00, 1},
	{fmt.Sprintf("RetryConfig%v", ExpController_00), ExpController_00, 2},
	{fmt.Sprintf("RetryConfig%v", ExpController_01), ExpController_01, 2},
}

func TestIncrement(t *testing.T) {
	for _, testArgs := range CtlIncrementTests {
		testArgs.InputController.IncDelay()
		actualCounter := testArgs.InputController.DelayCounter
		if actualCounter != testArgs.ExpectedCounter {
			t.Errorf("RetryConfig.Increment(%s): expected %d, actual %d\n", testArgs.Descriptor, testArgs.ExpectedCounter, actualCounter)
		}

	}
}