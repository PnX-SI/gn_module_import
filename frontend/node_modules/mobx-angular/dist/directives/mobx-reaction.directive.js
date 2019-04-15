var __extends = (this && this.__extends) || (function () {
    var extendStatics = Object.setPrototypeOf ||
        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
        function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
import { Directive, ViewContainerRef, TemplateRef, Input, Renderer } from '@angular/core';
import { reaction } from 'mobx';
import { MobxAutorunDirective } from './mobx-autorun.directive';
var MobxReactionDirective = /** @class */ (function (_super) {
    __extends(MobxReactionDirective, _super);
    function MobxReactionDirective(templateRef, viewContainer, renderer) {
        var _this = _super.call(this, templateRef, viewContainer, renderer) || this;
        _this.templateRef = templateRef;
        _this.viewContainer = viewContainer;
        _this.renderer = renderer;
        return _this;
    }
    MobxReactionDirective.prototype.autoDetect = function (view) {
        this.dispose = reaction(this.mobxReaction, function () {
            view['detectChanges']();
        }, { fireImmediately: true });
    };
    MobxReactionDirective.decorators = [
        { type: Directive, args: [{ selector: '[mobxReaction]' },] },
    ];
    /** @nocollapse */
    MobxReactionDirective.ctorParameters = function () { return [
        { type: TemplateRef, },
        { type: ViewContainerRef, },
        { type: Renderer, },
    ]; };
    MobxReactionDirective.propDecorators = {
        'mobxReaction': [{ type: Input },],
    };
    return MobxReactionDirective;
}(MobxAutorunDirective));
export { MobxReactionDirective };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIi4uLy4uL2xpYi9kaXJlY3RpdmVzL21vYngtcmVhY3Rpb24uZGlyZWN0aXZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7QUFBQSxPQUFPLEVBQUUsU0FBQSxFQUFXLGdCQUFBLEVBQWtCLFdBQUEsRUFBYSxLQUFBLEVBQU8sUUFBQSxFQUFTLE1BQU8sZUFBQSxDQUFnQjtBQUMxRixPQUFPLEVBQUUsUUFBQSxFQUFTLE1BQU8sTUFBQSxDQUFPO0FBQ2hDLE9BQU8sRUFBQSxvQkFBRSxFQUFvQixNQUFNLDBCQUFBLENBQTJCO0FBRzlEO0lBQTJDLHlDQUFvQjtJQUc3RCwrQkFDWSxXQUE2QixFQUM3QixhQUErQixFQUMvQixRQUFrQjtRQUg5QixZQUdpQyxrQkFBTSxXQUFXLEVBQUUsYUFBYSxFQUFFLFFBQVEsQ0FBQyxTQUFHO1FBRm5FLGlCQUFXLEdBQVgsV0FBVyxDQUFrQjtRQUM3QixtQkFBYSxHQUFiLGFBQWEsQ0FBa0I7UUFDL0IsY0FBUSxHQUFSLFFBQVEsQ0FBVTs7SUFBZ0QsQ0FBQztJQUUvRSwwQ0FBVSxHQUFWLFVBQVcsSUFBSTtRQUNiLElBQUksQ0FBQyxPQUFPLEdBQUcsUUFBUSxDQUFDLElBQUksQ0FBQyxZQUFZLEVBQUU7WUFDekMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxFQUFFLENBQUM7UUFDMUIsQ0FBQyxFQUFFLEVBQUUsZUFBZSxFQUFFLElBQUksRUFBRSxDQUFDLENBQUM7SUFDaEMsQ0FBQztJQUNJLGdDQUFVLEdBQTBCO1FBQzNDLEVBQUUsSUFBSSxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUsQ0FBQyxFQUFFLFFBQVEsRUFBRSxnQkFBZ0IsRUFBRSxFQUFHLEVBQUU7S0FDNUQsQ0FBQztJQUNGLGtCQUFrQjtJQUNYLG9DQUFjLEdBQW1FLGNBQU0sT0FBQTtRQUM5RixFQUFDLElBQUksRUFBRSxXQUFXLEdBQUc7UUFDckIsRUFBQyxJQUFJLEVBQUUsZ0JBQWdCLEdBQUc7UUFDMUIsRUFBQyxJQUFJLEVBQUUsUUFBUSxHQUFHO0tBQ2pCLEVBSjZGLENBSTdGLENBQUM7SUFDSyxvQ0FBYyxHQUEyQztRQUNoRSxjQUFjLEVBQUUsQ0FBQyxFQUFFLElBQUksRUFBRSxLQUFLLEVBQUUsRUFBRTtLQUNqQyxDQUFDO0lBQ0YsNEJBQUM7Q0F6QkQsQUF5QkMsQ0F6QjBDLG9CQUFvQixHQXlCOUQ7U0F6QlkscUJBQXFCIiwiZmlsZSI6Im1vYngtcmVhY3Rpb24uZGlyZWN0aXZlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IERpcmVjdGl2ZSwgVmlld0NvbnRhaW5lclJlZiwgVGVtcGxhdGVSZWYsIElucHV0LCBSZW5kZXJlciB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgcmVhY3Rpb24gfSBmcm9tICdtb2J4JztcbmltcG9ydCB7TW9ieEF1dG9ydW5EaXJlY3RpdmV9IGZyb20gJy4vbW9ieC1hdXRvcnVuLmRpcmVjdGl2ZSc7XG5cblxuZXhwb3J0IGNsYXNzIE1vYnhSZWFjdGlvbkRpcmVjdGl2ZSBleHRlbmRzIE1vYnhBdXRvcnVuRGlyZWN0aXZlIHtcbiAgIG1vYnhSZWFjdGlvbjtcblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcm90ZWN0ZWQgdGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPGFueT4sXG4gICAgcHJvdGVjdGVkIHZpZXdDb250YWluZXI6IFZpZXdDb250YWluZXJSZWYsXG4gICAgcHJvdGVjdGVkIHJlbmRlcmVyOiBSZW5kZXJlcikge3N1cGVyKHRlbXBsYXRlUmVmLCB2aWV3Q29udGFpbmVyLCByZW5kZXJlcik7IH1cblxuICBhdXRvRGV0ZWN0KHZpZXcpIHtcbiAgICB0aGlzLmRpc3Bvc2UgPSByZWFjdGlvbih0aGlzLm1vYnhSZWFjdGlvbiwgKCkgPT4ge1xuICAgICAgdmlld1snZGV0ZWN0Q2hhbmdlcyddKCk7XG4gICAgfSwgeyBmaXJlSW1tZWRpYXRlbHk6IHRydWUgfSk7XG4gIH1cbnN0YXRpYyBkZWNvcmF0b3JzOiBEZWNvcmF0b3JJbnZvY2F0aW9uW10gPSBbXG57IHR5cGU6IERpcmVjdGl2ZSwgYXJnczogW3sgc2VsZWN0b3I6ICdbbW9ieFJlYWN0aW9uXScgfSwgXSB9LFxuXTtcbi8qKiBAbm9jb2xsYXBzZSAqL1xuc3RhdGljIGN0b3JQYXJhbWV0ZXJzOiAoKSA9PiAoe3R5cGU6IGFueSwgZGVjb3JhdG9ycz86IERlY29yYXRvckludm9jYXRpb25bXX18bnVsbClbXSA9ICgpID0+IFtcbnt0eXBlOiBUZW1wbGF0ZVJlZiwgfSxcbnt0eXBlOiBWaWV3Q29udGFpbmVyUmVmLCB9LFxue3R5cGU6IFJlbmRlcmVyLCB9LFxuXTtcbnN0YXRpYyBwcm9wRGVjb3JhdG9yczoge1trZXk6IHN0cmluZ106IERlY29yYXRvckludm9jYXRpb25bXX0gPSB7XG4nbW9ieFJlYWN0aW9uJzogW3sgdHlwZTogSW5wdXQgfSxdLFxufTtcbn1cblxuaW50ZXJmYWNlIERlY29yYXRvckludm9jYXRpb24ge1xuICB0eXBlOiBGdW5jdGlvbjtcbiAgYXJncz86IGFueVtdO1xufVxuIl19