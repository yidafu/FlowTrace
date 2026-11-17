package dev.yidafu.flowtrace

import com.facebook.react.bridge.ReactApplicationContext

class FlowtraceModule(reactContext: ReactApplicationContext) :
  NativeFlowtraceSpec(reactContext) {

  override fun multiply(a: Double, b: Double): Double {
    return a * b
  }

  companion object {
    const val NAME = NativeFlowtraceSpec.NAME
  }
}
