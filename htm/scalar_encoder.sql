create or replace function SCALAR_ENCODER(INPUTNUMBER float, SDR_WIDTH float,MIN_VAL float,MAX_VAL float,WIDTH float, DENSE_OUTPUT)
  returns array
  language javascript
  immutable
as
$$
var scaleFunction = function(opts){
  var istart = opts.domain[0],
      istop  = opts.domain[1],
      ostart = opts.range[0],
      ostop  = opts.range[1];

  return function scaleFunction(value) {
    return ostart + (ostop - ostart) * ((value - istart) / (istop - istart));
  }
};
 
function applyBitmaskAtIndex(index, w,n,reverseScale, dense_output) {
    let out = [];
    let lowerValue = reverseScale(index - (w/2));
    let upperValue = reverseScale(index + (w/2))
 
    // For each bit in the encoding, we get the input domain
    // value. Using w, we know how wide the bitmask should
    // be, so we use the reverse scales to define the size
    // of the bitmask. If this index is within the value
    // range, we turn it on.
    for (let i = 0; i < n; i++) {
        let bitValue = reverseScale(i);
        let bitOut = 0;
        // if dense==true, push out the index value only when active
        // otherwise, always output 0 or 1 accordingly
        if (lowerValue <= bitValue && bitValue < upperValue) {
            if (dense_output){
              out.push(index)
            }
            bitOut = 1
        }
        if (!dense_output){
          out.push(bitOut)
        }
    }
    return out
}
// Accepts a scalar value within the input domain, returns an
// array of bits representing the value.
function encode(value,n,min,max,width) {
    let generatedScaleFunction = scaleFunction({domain: [min,max],range:[0,n]})
    let generatedReverseScaleFunction = scaleFunction({domain: [0,n],range:[min,max]})
    // Using the scale, get the corresponding integer
    // index for this value
    let index = Math.floor(generatedScaleFunction(value));
    if (index > n - 1) {
        index = n - 1
    }
    return applyBitmaskAtIndex(index,width,n,generatedReverseScaleFunction);
}
    return encode(INPUTNUMBER,SDR_WIDTH,MIN_VAL,MAX_VAL,WIDTH, DENSE_OUTPUT);
$$
;
