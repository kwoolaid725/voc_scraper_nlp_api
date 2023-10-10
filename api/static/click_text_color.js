// document.getElementById("change_color").onclick = function() {
//   // Get Selection
//   sel = window.getSelection();
//   if (sel.rangeCount && sel.getRangeAt) {
//     range = sel.getRangeAt(0);
//   }
//   // Set design mode to on
//   document.designMode = "on";
//   if (range) {
//     sel.removeAllRanges();
//     sel.addRange(range);
//   }
//   // Colorize text
//   document.execCommand("ForeColor", false, "red");
//   // Set design mode to off
//   document.designMode = "off";
// }
// <span id="content" contenteditable>
// Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent sit amet odio eu magna mattis vehicula. Duis egestas fermentum leo. Nunc eget dapibus eros, id egestas magna. Fusce non arcu non quam laoreet porttitor non non dui. Ut elit nisl, facilisis id hendrerit et, maximus at nunc. Fusce at consequat massa. Curabitur fermentum odio risus, vel egestas ligula rhoncus id. Nam pulvinar mollis consectetur. Aenean dictum ut tellus id fringilla. Maecenas rutrum ultrices leo, sed tincidunt massa tempus ac. Suspendisse potenti. Aenean eu tempus nisl.
// </span>
// <br/><br/>
// <button id="change_color">Change Selected Text Color</button>