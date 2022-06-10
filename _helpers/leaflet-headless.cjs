/**
 * Bare-bone leaflet-headless implementation
 * (original code from https://github.com/jieter/leaflet-headless)
 */

function HL() {
    const jsdom = require("jsdom")
    const { JSDOM } = jsdom

    const dom = new JSDOM(`<!DOCTYPE html><html><head></head><body></body></html>`)

    if (!global.L) {
        // make some globals to fake browser behaviour.
        global.window = dom.window
        global.document = global.window.document
        global.window.navigator.userAgent = 'webkit'
        global.navigator = global.window.navigator

        global.L_DISABLE_3D = true
        global.L_NO_TOUCH = true
    
        var leafletPath = require.resolve('leaflet')
        var L = require(leafletPath)
        global.L = L
        global.L.window = global.window
        global.L.document = global.document
    
        // Monkey patch Leaflet
        var originalInit = L.Map.prototype.initialize
        L.Map.prototype.initialize = function (id, options) {
            options = L.extend(options || {}, {
                fadeAnimation: false,
                zoomAnimation: false,
                markerZoomAnimation: false,
                preferCanvas: true
            })
    
            return originalInit.call(this, id, options)
        }
    
        // jsdom does not have clientHeight/clientWidth on elements.
        // Adjust size with L.Map.setSize()
        L.Map.prototype.getSize = function () {
            if (!this._size || this._sizeChanged) {
                this._size = new L.Point(1024, 1024)
                this._sizeChanged = false
            }
            return this._size.clone()
        }
    
        L.Map.prototype.setSize = function (width, height) {
            this._size = new L.Point(width, height)
            // reset pixelOrigin
            this._resetView(this.getCenter(), this.getZoom())
            return this
        }
    }
    return global.L
}
module.exports = HL
