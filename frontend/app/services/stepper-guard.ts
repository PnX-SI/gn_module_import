import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';

@Injectable()
export class StepperGuardService implements CanActivate {
	constructor(private router: Router) {}

	canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
		let url: string = state.url.split('?')[0];
		if (JSON.parse(localStorage.getItem('startPorcess'))) {
			if (url.endsWith('process/step/1')) {
				return true;
			} else if (url.endsWith('process/step/2')) {
				if (JSON.parse(localStorage.getItem('step2Data'))) return true;
				else return false;
			} else if (url.endsWith('process/step/3')) {
				let step3Data = JSON.parse(localStorage.getItem('step3Data'))
				let step2Data = JSON.parse(localStorage.getItem('step2Data'))
				if (step3Data && step2Data.mappingIsValidate) {
					return true;
				} else return false;
			} else if (url.endsWith('process/step/4')) {
				if (JSON.parse(localStorage.getItem('step4Data'))) return true;
				else return false;
			} else return false;
		} else {
			return false;
		}
	}
}
