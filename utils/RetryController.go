package utils

import (
	"time"
	"math"
	"math/rand"
	"errors"
)

type RetryController interface {
	IncDelay()
	ResetDelay()
	SetMaxTimeout(t time.Duration)
	SetRetryInterval(t time.Duration)
}

type JitterController interface {
	GetJitter()
}

type JitterConfig struct {
	// used for tuning the 
	Range     uint
	Interval  time.Duration
}

type RetryConfig struct {
	Mode            string
	JitterEnable    bool
	Jitter          *JitterConfig
	DelayScalar     uint 	// accelerates increase of delay between retries
	DelayCounter    uint

	RetryCounter	uint
	RetryCountMax   uint // 0, no max limit

	RetryInterval   time.Duration
	RetryTimer      time.Duration
	RetryTimerMax	time.Duration // 0, no max limit

	// RetryStartTime  time.???
	// RetryTimeout    time.Duration
}

// type RetryControllerConfig struct {
// 	jitterEnable bool
// 	Jitter       JitterConfig
// 	Timer        RetryConfig
// }

// func (rc *RetryController) OnRetryTimeout() {
// }

// func (rc *RetryController) OnRetryMax() {
// 	if rc.RetryCounter >= rc.RetryCountMax {
// 		// do something
// 	}
// }

func NewRetryConfig(mode string) (*RetryConfig, error) {
	// TODO: implement errors if no string provided

	rc := &RetryConfig{}

	// apply sane defaults
	switch mode {
	case "linear":
		rc.Mode = "linear"
		rc.DelayScalar = 1
		rc.JitterEnable = true
		rc.Jitter = &JitterConfig{
			Range: 10,
			Interval: time.Millisecond,
		}
		rc.RetryInterval = 10 * time.Millisecond
		rc.RetryTimerMax = 2 * time.Minute
		rc.RetryCountMax = 10
	
	case "exponential":
		rc.Mode = "exponential"
		rc.DelayScalar = 2
		rc.JitterEnable = true
		rc.Jitter = &JitterConfig{
			Range: 10,
			Interval: time.Millisecond,
		}
		rc.RetryInterval = 10 * time.Millisecond
		rc.RetryTimerMax = 2 * time.Minute
		rc.RetryCountMax = 10
	default:
		err := errors.New("Failed to create RetryConfig")
		return rc, err
	}

	return rc, nil
}

func (jc *JitterConfig) GetJitter() time.Duration {
	return time.Duration(rand.Intn( int(jc.Range) )) * jc.Interval
}

func (rc *RetryConfig) IncDelay() {

	switch rc.Mode {
	case "linear":
		if rc.DelayScalar < 1 {
			rc.DelayScalar = 1
		}
		rc.DelayCounter = rc.DelayCounter + rc.DelayScalar
	case "exponential":
		// DelayCounter cannot be zero when using exponential mode
		switch rc.DelayCounter {
		case 0:
			rc.ResetDelay()
		default:
			// DelayScalar used as exponent, and should not be < 2
			if rc.DelayScalar < 2 {
				rc.DelayScalar = 2
			}
			// math.pow expects float, but we use uint.
			// conversion is necessary here
			rc.DelayCounter = uint( math.Pow( float64(rc.DelayCounter), float64(rc.DelayScalar)	) )
			
			if rc.DelayCounter == 1 {
				// if DelayCounter is 1, it will never increase, 
				// so enable exponetial behavior with arbitrary increase:
				rc.DelayCounter = rc.DelayCounter + 1
			}
		}
	default:
		// default to linear behavior
		// log warning
		rc.Mode = "linear"
		rc.IncDelay()
	}
}

func (rc *RetryConfig) IncRetry(i uint) {
	rc.RetryCounter = rc.RetryCounter + 1
}

func (rc *RetryConfig) ResetDelay() {
	switch rc.Mode {
	case "linear":
		rc.DelayCounter = 1
	case "exponential":
		rc.DelayCounter = 2
	}
}

func (rc *RetryConfig) Wait() {

	if rc.JitterEnable {
		rc.RetryTimer = time.Duration(rc.DelayCounter) * rc.RetryInterval + rc.Jitter.GetJitter()
	} else {
		rc.RetryTimer = time.Duration(rc.DelayCounter) * rc.RetryInterval
	}

	if rc.RetryTimer >= rc.RetryTimerMax {
		time.Sleep(rc.RetryTimerMax)		
	} else {
		time.Sleep(rc.RetryTimer)
	}
}