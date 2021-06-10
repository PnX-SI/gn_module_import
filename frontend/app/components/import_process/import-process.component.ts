import { Component } from '@angular/core';
import { Router, ActivatedRoute, ActivatedRouteSnapshot, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';
import { ModuleConfig } from '../../module.config';
import { Step } from '../../models/enums.model';
import { Import } from '../../models/import.model';
import { ImportStepInterface } from './import-process.interface'

@Component({
	selector: 'import-process',
	styleUrls: ['import-process.component.scss'],
	templateUrl: 'import-process.component.html'
})
export class ImportProcessComponent {
  public step: Step;
  public stepComponent;

  constructor(
      private route: ActivatedRoute,
      private router: Router,
  ) {
    this.router.routeReuseStrategy.shouldReuseRoute = (future: ActivatedRouteSnapshot, current: ActivatedRouteSnapshot) => {
      if (future.routeConfig === current.routeConfig) {
        if (current.parent && current.parent.component === ImportProcessComponent) {
          // reset components on id_import changes
          return future.params.id_import == current.params.id_import;
        } else {
          return true;
        }
      } else {
          return false;
      }
    };
    this.router.events.pipe(
      filter(event => event instanceof NavigationEnd),
    ).subscribe(event => {
      this.step = this.route.snapshot.firstChild.data.step;
    });
  }

  onActivate(stepComponent) {
    this.stepComponent = stepComponent;
  }
}
