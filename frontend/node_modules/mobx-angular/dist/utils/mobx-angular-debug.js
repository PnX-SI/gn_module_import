/* tslint:disable:max-line-length */
/* tslint:disable:no-console */
import { extras, spy, isObservableArray, isObservableObject } from 'mobx';
// function for turning debug on / off
export var mobxAngularDebug = (function () {
    if (typeof localStorage === 'undefined' || typeof console === 'undefined' || typeof window === 'undefined') {
        return function () { };
    }
    if (!localStorage || !console || !window) {
        return function () { };
    }
    var style = 'background: #222; color: #bada55';
    window['mobxAngularDebug'] = function (value) {
        if (value) {
            console.log('%c MobX will now log everything to the console', style);
            console.log('%c Right-click any element to see its dependency tree', style);
            localStorage['mobx-angular-debug'] = true;
        }
        else
            delete localStorage['mobx-angular-debug'];
    };
    function isDebugOn() {
        return localStorage['mobx-angular-debug'];
    }
    spy(function (change) { return isDebugOn() && consoleLogChange(change, function () { return true; }); });
    // Debugging element dependency tree
    function mobxAngularDebug(view, renderer, observer) {
        var element = view.rootNodes[0];
        renderer.listen(element, 'contextmenu', function () {
            if (isDebugOn())
                console.log(extras.getDependencyTree(observer));
        });
    }
    /////////////////////////////////////////////////////////
    // console logging (copied from mobx-react)
    var advicedToUseChrome = false;
    var currentDepth = 0;
    var isInsideSkippedGroup = false;
    function consoleLogChange(change, filter) {
        if (advicedToUseChrome === false && typeof navigator !== 'undefined' && navigator.userAgent.indexOf('Chrome') === -1) {
            console.warn('The output of the MobX logger is optimized for Chrome');
            advicedToUseChrome = true;
        }
        var isGroupStart = change.spyReportStart === true;
        var isGroupEnd = change.spyReportEnd === true;
        var show;
        if (currentDepth === 0) {
            show = filter(change);
            if (isGroupStart && !show) {
                isInsideSkippedGroup = true;
            }
        }
        else if (isGroupEnd && isInsideSkippedGroup && currentDepth === 1) {
            show = false;
            isInsideSkippedGroup = false;
        }
        else {
            show = isInsideSkippedGroup !== true;
        }
        if (show && isGroupEnd) {
            groupEnd(change.time);
        }
        else if (show) {
            var logNext = isGroupStart ? group : log;
            switch (change.type) {
                case 'action':
                    // name, target, arguments, fn
                    logNext("%caction '%s' %s", 'color:dodgerblue', change.name, autoWrap('(', getNameForThis(change.target)));
                    log(change.arguments);
                    trace();
                    break;
                case 'transaction':
                    // name, target
                    logNext("%ctransaction '%s' %s", 'color:gray', change.name, autoWrap('(', getNameForThis(change.target)));
                    break;
                case 'scheduled-reaction':
                    // object
                    logNext("%cscheduled async reaction '%s'", 'color:#10a210', observableName(change.object));
                    break;
                case 'reaction':
                    // object, fn
                    logNext("%creaction '%s'", 'color:#10a210', observableName(change.object));
                    // dir({
                    //     fn: change.fn
                    // });
                    trace();
                    break;
                case 'compute':
                    // object, target, fn
                    group("%ccomputed '%s' %s", 'color:#10a210', observableName(change.object), autoWrap('(', getNameForThis(change.target)));
                    // dir({
                    //    fn: change.fn,
                    //    target: change.target
                    // });
                    groupEnd();
                    break;
                case 'error':
                    // message
                    logNext('%cerror: %s', 'color:tomato', change.message);
                    trace();
                    closeGroupsOnError();
                    break;
                case 'update':
                    // (array) object, index, newValue, oldValue
                    // (map, obbject) object, name, newValue, oldValue
                    // (value) object, newValue, oldValue
                    if (isObservableArray(change.object)) {
                        logNext('updated \'%s[%s]\': %s (was: %s)', observableName(change.object), change.index, formatValue(change.newValue), formatValue(change.oldValue));
                    }
                    else if (isObservableObject(change.object)) {
                        logNext('updated \'%s.%s\': %s (was: %s)', observableName(change.object), change.name, formatValue(change.newValue), formatValue(change.oldValue));
                    }
                    else {
                        logNext('updated \'%s\': %s (was: %s)', observableName(change.object), change.name, formatValue(change.newValue), formatValue(change.oldValue));
                    }
                    dir({
                        newValue: change.newValue,
                        oldValue: change.oldValue
                    });
                    trace();
                    break;
                case 'splice':
                    // (array) object, index, added, removed, addedCount, removedCount
                    logNext('spliced \'%s\': index %d, added %d, removed %d', observableName(change.object), change.index, change.addedCount, change.removedCount);
                    dir({
                        added: change.added,
                        removed: change.removed
                    });
                    trace();
                    break;
                case 'add':
                    // (map, object) object, name, newValue
                    logNext('set \'%s.%s\': %s', observableName(change.object), change.name, formatValue(change.newValue));
                    dir({
                        newValue: change.newValue
                    });
                    trace();
                    break;
                case 'delete':
                    // (map) object, name, oldValue
                    logNext('removed \'%s.%s\' (was %s)', observableName(change.object), change.name, formatValue(change.oldValue));
                    dir({
                        oldValue: change.oldValue
                    });
                    trace();
                    break;
                case 'create':
                    // (value) object, newValue
                    logNext('set \'%s\': %s', observableName(change.object), formatValue(change.newValue));
                    dir({
                        newValue: change.newValue
                    });
                    trace();
                    break;
                default:
                    // generic fallback for future events
                    logNext(change.type);
                    dir(change);
                    break;
            }
        }
        if (isGroupStart)
            currentDepth++;
        if (isGroupEnd)
            currentDepth--;
    }
    var consoleSupportsGroups = false; // typeof console.groupCollapsed === 'function';
    var currentlyLoggedDepth = 0;
    function group() {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        // TODO: firefox does not support formatting in groupStart methods..
        consoleSupportsGroups ?
            console.groupCollapsed.apply(console, args) :
            console.log.apply(console, args);
        currentlyLoggedDepth++;
    }
    function groupEnd(time) {
        currentlyLoggedDepth--;
        if (typeof time === 'number') {
            log('%ctotal time: %sms', 'color:gray', time);
        }
        if (consoleSupportsGroups)
            console.groupEnd();
    }
    function log() {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        console.log.apply(console, args);
    }
    function dir() {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        console.dir.apply(console, args);
    }
    function trace() {
        // TODO: needs wrapping in firefox?
        if (console.trace) {
            console.trace('stack'); // TODO: use stacktrace.js or similar and strip off unrelevant stuff?
        }
    }
    function closeGroupsOnError() {
        for (var i = 0, m = currentlyLoggedDepth; i < m; i++)
            groupEnd();
    }
    var closeToken = {
        '"': '"',
        '\'': '\'',
        '(': ')',
        '[': ']',
        '<': ']',
        '#': ''
    };
    function autoWrap(token, value) {
        if (!value)
            return '';
        return (token || '') + value + (closeToken[token] || '');
    }
    function observableName(object) {
        return extras.getDebugName(object);
    }
    function formatValue(value) {
        if (isPrimitive(value)) {
            if (typeof value === 'string' && value.length > 100)
                return value.substr(0, 97) + '...';
            return value;
        }
        else
            return autoWrap('(', getNameForThis(value));
    }
    function getNameForThis(who) {
        if (who === null || who === undefined) {
            return '';
        }
        else if (who && typeof who === 'object') {
            if (who && who.$mobx) {
                return who.$mobx.name;
            }
            else if (who.constructor) {
                return who.constructor.name || 'object';
            }
        }
        return "" + typeof who;
    }
    function isPrimitive(value) {
        return value === null || value === undefined || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
    }
    return mobxAngularDebug;
})();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIi4uLy4uL2xpYi91dGlscy9tb2J4LWFuZ3VsYXItZGVidWcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUEsb0NBQW9DO0FBQ3BDLCtCQUErQjtBQUMvQixPQUFPLEVBQUUsTUFBTSxFQUFFLEdBQUcsRUFBRSxpQkFBaUIsRUFBRSxrQkFBa0IsRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUUxRSxzQ0FBc0M7QUFDdEMsTUFBTSxDQUFDLElBQU0sZ0JBQWdCLEdBQUcsQ0FBQztJQUMvQixFQUFFLENBQUMsQ0FBQyxPQUFPLFlBQVksS0FBSyxXQUFXLElBQUksT0FBTyxPQUFPLEtBQUssV0FBVyxJQUFJLE9BQU8sTUFBTSxLQUFLLFdBQVcsQ0FBQyxDQUFDLENBQUM7UUFDM0csTUFBTSxDQUFDLGNBQU8sQ0FBQyxDQUFDO0lBQ2xCLENBQUM7SUFFRCxFQUFFLENBQUMsQ0FBQyxDQUFDLFlBQVksSUFBSSxDQUFDLE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUM7UUFDekMsTUFBTSxDQUFDLGNBQU8sQ0FBQyxDQUFDO0lBQ2xCLENBQUM7SUFFRCxJQUFNLEtBQUssR0FBRyxrQ0FBa0MsQ0FBQztJQUVqRCxNQUFNLENBQUMsa0JBQWtCLENBQUMsR0FBRyxVQUFDLEtBQUs7UUFDakMsRUFBRSxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztZQUNSLE9BQU8sQ0FBQyxHQUFHLENBQUMsZ0RBQWdELEVBQUUsS0FBSyxDQUFDLENBQUM7WUFDckUsT0FBTyxDQUFDLEdBQUcsQ0FBQyx1REFBdUQsRUFBRSxLQUFLLENBQUMsQ0FBQztZQUM1RSxZQUFZLENBQUMsb0JBQW9CLENBQUMsR0FBRyxJQUFJLENBQUM7UUFDOUMsQ0FBQztRQUNELElBQUk7WUFBQyxPQUFPLFlBQVksQ0FBQyxvQkFBb0IsQ0FBQyxDQUFDO0lBQ2pELENBQUMsQ0FBQztJQUVGO1FBQ0UsTUFBTSxDQUFDLFlBQVksQ0FBQyxvQkFBb0IsQ0FBQyxDQUFDO0lBQzVDLENBQUM7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SztRQUN4QixNQUFNLENBQUMsS0FBSyxLQUFLLElBQUksSUFBSSxLQUFLLEtBQUssU0FBUyxJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRLElBQUksT0FBTyxLQUFLLEtBQUssU0FBUyxDQUFDO0lBQ3ZJLENBQUM7SUFFRCxNQUFNLENBQUMsZ0JBQWdCLENBQUM7QUFDMUIsQ0FBQyxDQUFDLEVBQUUsQ0FBQyIsImZpbGUiOiJtb2J4LWFuZ3VsYXItZGVidWcuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlc0NvbnRlbnQiOlsiLyogdHNsaW50OmRpc2FibGU6bWF4LWxpbmUtbGVuZ3RoICovXG4vKiB0c2xpbnQ6ZGlzYWJsZTpuby1jb25zb2xlICovXG5pbXBvcnQgeyBleHRyYXMsIHNweSwgaXNPYnNlcnZhYmxlQXJyYXksIGlzT2JzZXJ2YWJsZU9iamVjdCB9IGZyb20gJ21vYngnO1xuXG4vLyBmdW5jdGlvbiBmb3IgdHVybmluZyBkZWJ1ZyBvbiAvIG9mZlxuZXhwb3J0IGNvbnN0IG1vYnhBbmd1bGFyRGVidWcgPSAoKCkgPT4ge1xuICBpZiAodHlwZW9mIGxvY2FsU3RvcmFnZSA9PT0gJ3VuZGVmaW5lZCcgfHwgdHlwZW9mIGNvbnNvbGUgPT09ICd1bmRlZmluZWQnIHx8IHR5cGVvZiB3aW5kb3cgPT09ICd1bmRlZmluZWQnKSB7XG4gICAgcmV0dXJuICgpID0+IHt9O1xuICB9XG5cbiAgaWYgKCFsb2NhbFN0b3JhZ2UgfHwgIWNvbnNvbGUgfHwgIXdpbmRvdykge1xuICAgIHJldHVybiAoKSA9PiB7fTtcbiAgfVxuXG4gIGNvbnN0IHN0eWxlID0gJ2JhY2tncm91bmQ6ICMyMjI7IGNvbG9yOiAjYmFkYTU1JztcblxuICB3aW5kb3dbJ21vYnhBbmd1bGFyRGVidWcnXSA9ICh2YWx1ZSkgPT4ge1xuICAgIGlmICh2YWx1ZSkge1xuICAgICAgICBjb25zb2xlLmxvZygnJWMgTW9iWCB3aWxsIG5vdyBsb2cgZXZlcnl0aGluZyB0byB0aGUgY29uc29sZScsIHN0eWxlKTtcbiAgICAgICAgY29uc29sZS5sb2coJyVjIFJpZ2h0LWNsaWNrIGFueSBlbGVtZW50IHRvIHNlZSBpdHMgZGVwZW5kZW5jeSB0cmVlJywgc3R5bGUpO1xuICAgICAgICBsb2NhbFN0b3JhZ2VbJ21vYngtYW5ndWxhci1kZWJ1ZyddID0gdHJ1ZTtcbiAgICB9XG4gICAgZWxzZSBkZWxldGUgbG9jYWxTdG9yYWdlWydtb2J4LWFuZ3VsYXItZGVidWcnXTtcbiAgfTtcblxuICBmdW5jdGlvbiBpc0RlYnVnT24oKSB7XG4gICAgcmV0dXJuIGxvY2FsU3RvcmFnZVsnbW9ieC1hbmd1bGFyLWRlYnVnJ107XG4gIH1cblxuICBzcHkoKGNoYW5nZSkgPT4gaXNEZWJ1Z09uKCkgJiYgY29uc29sZUxvZ0NoYW5nZShjaGFuZ2UsICgpID0+IHRydWUpKTtcblxuICAvLyBEZWJ1Z2dpbmcgZWxlbWVudCBkZXBlbmRlbmN5IHRyZWVcbiAgZnVuY3Rpb24gbW9ieEFuZ3VsYXJEZWJ1Zyh2aWV3LCByZW5kZXJlciwgb2JzZXJ2ZXIpIHtcbiAgICBjb25zdCBlbGVtZW50ID0gdmlldy5yb290Tm9kZXNbMF07XG5cbiAgICByZW5kZXJlci5saXN0ZW4oZWxlbWVudCwgJ2NvbnRleHRtZW51JywgKCkgPT4ge1xuICAgICAgICBpZiAoaXNEZWJ1Z09uKCkpIGNvbnNvbGUubG9nKGV4dHJhcy5nZXREZXBlbmRlbmN5VHJlZShvYnNlcnZlcikpO1xuICAgIH0pO1xuICB9XG5cbiAgLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vXG4gIC8vIGNvbnNvbGUgbG9nZ2luZyAoY29waWVkIGZyb20gbW9ieC1yZWFjdClcbiAgbGV0IGFkdmljZWRUb1VzZUNocm9tZSA9IGZhbHNlO1xuXG4gIGxldCBjdXJyZW50RGVwdGggPSAwO1xuICBsZXQgaXNJbnNpZGVTa2lwcGVkR3JvdXAgPSBmYWxzZTtcblxuICBmdW5jdGlvbiBjb25zb2xlTG9nQ2hhbmdlKGNoYW5nZSwgZmlsdGVyKSB7XG5cbiAgICAgIGlmIChhZHZpY2VkVG9Vc2VDaHJvbWUgPT09IGZhbHNlICYmIHR5cGVvZiBuYXZpZ2F0b3IgIT09ICd1bmRlZmluZWQnICYmIG5hdmlnYXRvci51c2VyQWdlbnQuaW5kZXhPZignQ2hyb21lJykgPT09IC0xKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKCdUaGUgb3V0cHV0IG9mIHRoZSBNb2JYIGxvZ2dlciBpcyBvcHRpbWl6ZWQgZm9yIENocm9tZScpO1xuICAgICAgICAgIGFkdmljZWRUb1VzZUNocm9tZSA9IHRydWU7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGlzR3JvdXBTdGFydCA9IGNoYW5nZS5zcHlSZXBvcnRTdGFydCA9PT0gdHJ1ZTtcbiAgICAgIGNvbnN0IGlzR3JvdXBFbmQgPSBjaGFuZ2Uuc3B5UmVwb3J0RW5kID09PSB0cnVlO1xuXG4gICAgICBsZXQgc2hvdztcbiAgICAgIGlmIChjdXJyZW50RGVwdGggPT09IDApIHtcbiAgICAgICAgICBzaG93ID0gZmlsdGVyKGNoYW5nZSk7XG4gICAgICAgICAgaWYgKGlzR3JvdXBTdGFydCAmJiAhc2hvdykgeyBpc0luc2lkZVNraXBwZWRHcm91cCA9IHRydWU7IH1cbiAgICAgIH0gZWxzZSBpZiAoaXNHcm91cEVuZCAmJiBpc0luc2lkZVNraXBwZWRHcm91cCAmJiBjdXJyZW50RGVwdGggPT09IDEpIHtcbiAgICAgICAgICBzaG93ID0gZmFsc2U7XG4gICAgICAgICAgaXNJbnNpZGVTa2lwcGVkR3JvdXAgPSBmYWxzZTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgc2hvdyA9IGlzSW5zaWRlU2tpcHBlZEdyb3VwICE9PSB0cnVlO1xuICAgICAgfVxuXG4gICAgICBpZiAoc2hvdyAmJiBpc0dyb3VwRW5kKSB7XG4gICAgICAgICAgZ3JvdXBFbmQoY2hhbmdlLnRpbWUpO1xuICAgICAgfSBlbHNlIGlmIChzaG93KSB7XG4gICAgICAgICAgY29uc3QgbG9nTmV4dDogYW55ID0gaXNHcm91cFN0YXJ0ID8gZ3JvdXAgOiBsb2c7XG4gICAgICAgICAgc3dpdGNoIChjaGFuZ2UudHlwZSkge1xuICAgICAgICAgICAgICBjYXNlICdhY3Rpb24nOlxuICAgICAgICAgICAgICAgICAgLy8gbmFtZSwgdGFyZ2V0LCBhcmd1bWVudHMsIGZuXG4gICAgICAgICAgICAgICAgICBsb2dOZXh0KGAlY2FjdGlvbiAnJXMnICVzYCwgJ2NvbG9yOmRvZGdlcmJsdWUnLCBjaGFuZ2UubmFtZSwgYXV0b1dyYXAoJygnLCBnZXROYW1lRm9yVGhpcyhjaGFuZ2UudGFyZ2V0KSkpO1xuICAgICAgICAgICAgICAgICAgbG9nKGNoYW5nZS5hcmd1bWVudHMpO1xuICAgICAgICAgICAgICAgICAgdHJhY2UoKTtcbiAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICBjYXNlICd0cmFuc2FjdGlvbic6XG4gICAgICAgICAgICAgICAgICAvLyBuYW1lLCB0YXJnZXRcbiAgICAgICAgICAgICAgICAgIGxvZ05leHQoYCVjdHJhbnNhY3Rpb24gJyVzJyAlc2AsICdjb2xvcjpncmF5JywgY2hhbmdlLm5hbWUsIGF1dG9XcmFwKCcoJywgZ2V0TmFtZUZvclRoaXMoY2hhbmdlLnRhcmdldCkpKTtcbiAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICBjYXNlICdzY2hlZHVsZWQtcmVhY3Rpb24nOlxuICAgICAgICAgICAgICAgICAgLy8gb2JqZWN0XG4gICAgICAgICAgICAgICAgICBsb2dOZXh0KGAlY3NjaGVkdWxlZCBhc3luYyByZWFjdGlvbiAnJXMnYCwgJ2NvbG9yOiMxMGEyMTAnLCBvYnNlcnZhYmxlTmFtZShjaGFuZ2Uub2JqZWN0KSk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgY2FzZSAncmVhY3Rpb24nOlxuICAgICAgICAgICAgICAgICAgLy8gb2JqZWN0LCBmblxuICAgICAgICAgICAgICAgICAgbG9nTmV4dChgJWNyZWFjdGlvbiAnJXMnYCwgJ2NvbG9yOiMxMGEyMTAnLCBvYnNlcnZhYmxlTmFtZShjaGFuZ2Uub2JqZWN0KSk7XG4gICAgICAgICAgICAgICAgICAvLyBkaXIoe1xuICAgICAgICAgICAgICAgICAgLy8gICAgIGZuOiBjaGFuZ2UuZm5cbiAgICAgICAgICAgICAgICAgIC8vIH0pO1xuICAgICAgICAgICAgICAgICAgdHJhY2UoKTtcbiAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICBjYXNlICdjb21wdXRlJzpcbiAgICAgICAgICAgICAgICAgIC8vIG9iamVjdCwgdGFyZ2V0LCBmblxuICAgICAgICAgICAgICAgICAgZ3JvdXAoYCVjY29tcHV0ZWQgJyVzJyAlc2AsICdjb2xvcjojMTBhMjEwJywgb2JzZXJ2YWJsZU5hbWUoY2hhbmdlLm9iamVjdCksIGF1dG9XcmFwKCcoJywgZ2V0TmFtZUZvclRoaXMoY2hhbmdlLnRhcmdldCkpKTtcbiAgICAgICAgICAgICAgICAgIC8vIGRpcih7XG4gICAgICAgICAgICAgICAgICAvLyAgICBmbjogY2hhbmdlLmZuLFxuICAgICAgICAgICAgICAgICAgLy8gICAgdGFyZ2V0OiBjaGFuZ2UudGFyZ2V0XG4gICAgICAgICAgICAgICAgICAvLyB9KTtcbiAgICAgICAgICAgICAgICAgIGdyb3VwRW5kKCk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgY2FzZSAnZXJyb3InOlxuICAgICAgICAgICAgICAgICAgLy8gbWVzc2FnZVxuICAgICAgICAgICAgICAgICAgbG9nTmV4dCgnJWNlcnJvcjogJXMnLCAnY29sb3I6dG9tYXRvJywgY2hhbmdlLm1lc3NhZ2UpO1xuICAgICAgICAgICAgICAgICAgdHJhY2UoKTtcbiAgICAgICAgICAgICAgICAgIGNsb3NlR3JvdXBzT25FcnJvcigpO1xuICAgICAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICAgIGNhc2UgJ3VwZGF0ZSc6XG4gICAgICAgICAgICAgICAgICAvLyAoYXJyYXkpIG9iamVjdCwgaW5kZXgsIG5ld1ZhbHVlLCBvbGRWYWx1ZVxuICAgICAgICAgICAgICAgICAgLy8gKG1hcCwgb2JiamVjdCkgb2JqZWN0LCBuYW1lLCBuZXdWYWx1ZSwgb2xkVmFsdWVcbiAgICAgICAgICAgICAgICAgIC8vICh2YWx1ZSkgb2JqZWN0LCBuZXdWYWx1ZSwgb2xkVmFsdWVcbiAgICAgICAgICAgICAgICAgIGlmIChpc09ic2VydmFibGVBcnJheShjaGFuZ2Uub2JqZWN0KSkge1xuICAgICAgICAgICAgICAgICAgICAgIGxvZ05leHQoJ3VwZGF0ZWQgXFwnJXNbJXNdXFwnOiAlcyAod2FzOiAlcyknLCBvYnNlcnZhYmxlTmFtZShjaGFuZ2Uub2JqZWN0KSwgY2hhbmdlLmluZGV4LCBmb3JtYXRWYWx1ZShjaGFuZ2UubmV3VmFsdWUpLCBmb3JtYXRWYWx1ZShjaGFuZ2Uub2xkVmFsdWUpKTtcbiAgICAgICAgICAgICAgICAgIH0gZWxzZSBpZiAoaXNPYnNlcnZhYmxlT2JqZWN0KGNoYW5nZS5vYmplY3QpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgbG9nTmV4dCgndXBkYXRlZCBcXCclcy4lc1xcJzogJXMgKHdhczogJXMpJywgb2JzZXJ2YWJsZU5hbWUoY2hhbmdlLm9iamVjdCksIGNoYW5nZS5uYW1lLCBmb3JtYXRWYWx1ZShjaGFuZ2UubmV3VmFsdWUpLCBmb3JtYXRWYWx1ZShjaGFuZ2Uub2xkVmFsdWUpKTtcbiAgICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgbG9nTmV4dCgndXBkYXRlZCBcXCclc1xcJzogJXMgKHdhczogJXMpJywgb2JzZXJ2YWJsZU5hbWUoY2hhbmdlLm9iamVjdCksIGNoYW5nZS5uYW1lLCBmb3JtYXRWYWx1ZShjaGFuZ2UubmV3VmFsdWUpLCBmb3JtYXRWYWx1ZShjaGFuZ2Uub2xkVmFsdWUpKTtcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgIGRpcih7XG4gICAgICAgICAgICAgICAgICAgICAgbmV3VmFsdWU6IGNoYW5nZS5uZXdWYWx1ZSxcbiAgICAgICAgICAgICAgICAgICAgICBvbGRWYWx1ZTogY2hhbmdlLm9sZFZhbHVlXG4gICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgIHRyYWNlKCk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgY2FzZSAnc3BsaWNlJzpcbiAgICAgICAgICAgICAgICAgIC8vIChhcnJheSkgb2JqZWN0LCBpbmRleCwgYWRkZWQsIHJlbW92ZWQsIGFkZGVkQ291bnQsIHJlbW92ZWRDb3VudFxuICAgICAgICAgICAgICAgICAgbG9nTmV4dCgnc3BsaWNlZCBcXCclc1xcJzogaW5kZXggJWQsIGFkZGVkICVkLCByZW1vdmVkICVkJywgb2JzZXJ2YWJsZU5hbWUoY2hhbmdlLm9iamVjdCksIGNoYW5nZS5pbmRleCwgY2hhbmdlLmFkZGVkQ291bnQsIGNoYW5nZS5yZW1vdmVkQ291bnQpO1xuICAgICAgICAgICAgICAgICAgZGlyKHtcbiAgICAgICAgICAgICAgICAgICAgICBhZGRlZDogY2hhbmdlLmFkZGVkLFxuICAgICAgICAgICAgICAgICAgICAgIHJlbW92ZWQ6IGNoYW5nZS5yZW1vdmVkXG4gICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgIHRyYWNlKCk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgY2FzZSAnYWRkJzpcbiAgICAgICAgICAgICAgICAgIC8vIChtYXAsIG9iamVjdCkgb2JqZWN0LCBuYW1lLCBuZXdWYWx1ZVxuICAgICAgICAgICAgICAgICAgbG9nTmV4dCgnc2V0IFxcJyVzLiVzXFwnOiAlcycsIG9ic2VydmFibGVOYW1lKGNoYW5nZS5vYmplY3QpLCBjaGFuZ2UubmFtZSwgZm9ybWF0VmFsdWUoY2hhbmdlLm5ld1ZhbHVlKSk7XG4gICAgICAgICAgICAgICAgICBkaXIoe1xuICAgICAgICAgICAgICAgICAgICAgIG5ld1ZhbHVlOiBjaGFuZ2UubmV3VmFsdWVcbiAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgdHJhY2UoKTtcbiAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICBjYXNlICdkZWxldGUnOlxuICAgICAgICAgICAgICAgICAgLy8gKG1hcCkgb2JqZWN0LCBuYW1lLCBvbGRWYWx1ZVxuICAgICAgICAgICAgICAgICAgbG9nTmV4dCgncmVtb3ZlZCBcXCclcy4lc1xcJyAod2FzICVzKScsIG9ic2VydmFibGVOYW1lKGNoYW5nZS5vYmplY3QpLCBjaGFuZ2UubmFtZSwgZm9ybWF0VmFsdWUoY2hhbmdlLm9sZFZhbHVlKSk7XG4gICAgICAgICAgICAgICAgICBkaXIoe1xuICAgICAgICAgICAgICAgICAgICAgIG9sZFZhbHVlOiBjaGFuZ2Uub2xkVmFsdWVcbiAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgdHJhY2UoKTtcbiAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICBjYXNlICdjcmVhdGUnOlxuICAgICAgICAgICAgICAgICAgLy8gKHZhbHVlKSBvYmplY3QsIG5ld1ZhbHVlXG4gICAgICAgICAgICAgICAgICBsb2dOZXh0KCdzZXQgXFwnJXNcXCc6ICVzJywgb2JzZXJ2YWJsZU5hbWUoY2hhbmdlLm9iamVjdCksIGZvcm1hdFZhbHVlKGNoYW5nZS5uZXdWYWx1ZSkpO1xuICAgICAgICAgICAgICAgICAgZGlyKHtcbiAgICAgICAgICAgICAgICAgICAgICBuZXdWYWx1ZTogY2hhbmdlLm5ld1ZhbHVlXG4gICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgIHRyYWNlKCk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICAgICAgICAgIC8vIGdlbmVyaWMgZmFsbGJhY2sgZm9yIGZ1dHVyZSBldmVudHNcbiAgICAgICAgICAgICAgICAgIGxvZ05leHQoY2hhbmdlLnR5cGUpO1xuICAgICAgICAgICAgICAgICAgZGlyKGNoYW5nZSk7XG4gICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGlmIChpc0dyb3VwU3RhcnQpIGN1cnJlbnREZXB0aCsrO1xuICAgICAgaWYgKGlzR3JvdXBFbmQpIGN1cnJlbnREZXB0aC0tO1xuICB9XG5cbiAgY29uc3QgY29uc29sZVN1cHBvcnRzR3JvdXBzID0gZmFsc2U7IC8vIHR5cGVvZiBjb25zb2xlLmdyb3VwQ29sbGFwc2VkID09PSAnZnVuY3Rpb24nO1xuICBsZXQgY3VycmVudGx5TG9nZ2VkRGVwdGggPSAwO1xuXG4gIGZ1bmN0aW9uIGdyb3VwKC4uLmFyZ3MpIHtcbiAgICAgIC8vIFRPRE86IGZpcmVmb3ggZG9lcyBub3Qgc3VwcG9ydCBmb3JtYXR0aW5nIGluIGdyb3VwU3RhcnQgbWV0aG9kcy4uXG4gICAgICBjb25zb2xlU3VwcG9ydHNHcm91cHMgP1xuICAgICAgICBjb25zb2xlLmdyb3VwQ29sbGFwc2VkLmFwcGx5KGNvbnNvbGUsIGFyZ3MpIDpcbiAgICAgICAgY29uc29sZS5sb2cuYXBwbHkoY29uc29sZSwgYXJncyk7XG4gICAgICBjdXJyZW50bHlMb2dnZWREZXB0aCsrO1xuICB9XG5cbiAgZnVuY3Rpb24gZ3JvdXBFbmQodGltZT8pIHtcbiAgICAgIGN1cnJlbnRseUxvZ2dlZERlcHRoLS07XG4gICAgICBpZiAodHlwZW9mIHRpbWUgPT09ICdudW1iZXInKSB7XG4gICAgICAgICAgbG9nKCclY3RvdGFsIHRpbWU6ICVzbXMnLCAnY29sb3I6Z3JheScsIHRpbWUpO1xuICAgICAgfVxuICAgICAgaWYgKGNvbnNvbGVTdXBwb3J0c0dyb3VwcylcbiAgICAgICAgICBjb25zb2xlLmdyb3VwRW5kKCk7XG4gIH1cblxuICBmdW5jdGlvbiBsb2coLi4uYXJncykge1xuICAgICAgY29uc29sZS5sb2cuYXBwbHkoY29uc29sZSwgYXJncyk7XG4gIH1cblxuICBmdW5jdGlvbiBkaXIoLi4uYXJncykge1xuICAgICAgY29uc29sZS5kaXIuYXBwbHkoY29uc29sZSwgYXJncyk7XG4gIH1cblxuICBmdW5jdGlvbiB0cmFjZSgpIHtcbiAgICAgIC8vIFRPRE86IG5lZWRzIHdyYXBwaW5nIGluIGZpcmVmb3g/XG4gICAgaWYgKGNvbnNvbGUudHJhY2UpIHtcbiAgICAgIGNvbnNvbGUudHJhY2UoJ3N0YWNrJyk7IC8vIFRPRE86IHVzZSBzdGFja3RyYWNlLmpzIG9yIHNpbWlsYXIgYW5kIHN0cmlwIG9mZiB1bnJlbGV2YW50IHN0dWZmP1xuICAgIH1cbiAgfVxuXG4gIGZ1bmN0aW9uIGNsb3NlR3JvdXBzT25FcnJvcigpIHtcbiAgICAgIGZvciAobGV0IGkgPSAwLCBtID0gY3VycmVudGx5TG9nZ2VkRGVwdGg7IGkgPCBtOyBpKyspXG4gICAgICAgICAgZ3JvdXBFbmQoKTtcbiAgfVxuXG4gIGNvbnN0IGNsb3NlVG9rZW4gPSB7XG4gICAgICAnXCInIDogJ1wiJyxcbiAgICAgICdcXCcnIDogJ1xcJycsXG4gICAgICAnKCcgOiAnKScsXG4gICAgICAnWycgOiAnXScsXG4gICAgICAnPCcgOiAnXScsXG4gICAgICAnIycgOiAnJ1xuICB9O1xuXG4gIGZ1bmN0aW9uIGF1dG9XcmFwKHRva2VuLCB2YWx1ZSkge1xuICAgICAgaWYgKCF2YWx1ZSlcbiAgICAgICAgICByZXR1cm4gJyc7XG4gICAgICByZXR1cm4gKHRva2VuIHx8ICcnKSArIHZhbHVlICsgKGNsb3NlVG9rZW5bdG9rZW5dIHx8ICcnKTtcbiAgfVxuXG4gIGZ1bmN0aW9uIG9ic2VydmFibGVOYW1lKG9iamVjdCkge1xuICAgICAgcmV0dXJuIGV4dHJhcy5nZXREZWJ1Z05hbWUob2JqZWN0KTtcbiAgfVxuXG4gIGZ1bmN0aW9uIGZvcm1hdFZhbHVlKHZhbHVlKSB7XG4gICAgICBpZiAoaXNQcmltaXRpdmUodmFsdWUpKSB7XG4gICAgICAgICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycgJiYgdmFsdWUubGVuZ3RoID4gMTAwKVxuICAgICAgICAgICAgICByZXR1cm4gdmFsdWUuc3Vic3RyKDAsIDk3KSArICcuLi4nO1xuICAgICAgICAgIHJldHVybiB2YWx1ZTtcbiAgICAgIH0gZWxzZVxuICAgICAgICAgIHJldHVybiBhdXRvV3JhcCgnKCcsIGdldE5hbWVGb3JUaGlzKHZhbHVlKSk7XG4gIH1cblxuICBmdW5jdGlvbiBnZXROYW1lRm9yVGhpcyh3aG8pIHtcbiAgICAgIGlmICh3aG8gPT09IG51bGwgfHwgd2hvID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICByZXR1cm4gJyc7XG4gICAgICB9IGVsc2UgaWYgKHdobyAmJiB0eXBlb2Ygd2hvID09PSAnb2JqZWN0Jykge1xuICAgICAgICBpZiAod2hvICYmIHdoby4kbW9ieCkge1xuICAgICAgICAgIHJldHVybiB3aG8uJG1vYngubmFtZTtcbiAgICAgICAgICB9IGVsc2UgaWYgKHdoby5jb25zdHJ1Y3Rvcikge1xuICAgICAgICAgICAgICByZXR1cm4gd2hvLmNvbnN0cnVjdG9yLm5hbWUgfHwgJ29iamVjdCc7XG4gICAgICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gYCR7dHlwZW9mIHdob31gO1xuICB9XG5cbiAgZnVuY3Rpb24gaXNQcmltaXRpdmUodmFsdWUpIHtcbiAgICByZXR1cm4gdmFsdWUgPT09IG51bGwgfHwgdmFsdWUgPT09IHVuZGVmaW5lZCB8fCB0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnIHx8IHR5cGVvZiB2YWx1ZSA9PT0gJ251bWJlcicgfHwgdHlwZW9mIHZhbHVlID09PSAnYm9vbGVhbic7XG4gIH1cblxuICByZXR1cm4gbW9ieEFuZ3VsYXJEZWJ1Zztcbn0pKCk7XG4iXX0=