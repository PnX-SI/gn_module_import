import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { DataService } from "../services/data.service"
import { StepsService, Step2Data, Step3Data, Step4Data } from "../components/import_process/steps.service"

@Injectable()
export class StepperGuardService implements CanActivate {
	constructor(public stepService: StepsService, private router: Router, private _ds: DataService) { }

	canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean | Observable<boolean> {

		const idImport = route.params.id_import;

		const step: string = route.url[route.url.length - 1].path;


		//if (route.queryParams["resetStepper"] || JSON.parse(localStorage.getItem('startPorcess'))) {
		if (step == '1') {
			return true;
		} else if (step == '2') {
			if (JSON.parse(localStorage.getItem('step2Data'))) {
				return true
			} else {
				return new Observable<boolean>((stepperGuard) => {
					this._ds.getOneImport(idImport).subscribe(importObj => {
						if (importObj.step < step) {
							stepperGuard.next(false);
							stepperGuard.complete();
						} else {
							const step2: Step2Data = {
								importId: importObj.id_import,
								id_field_mapping: importObj.id_field_mapping,
								mappingIsValidate: importObj.step >= 2
							}
							this.stepService.setStepData(2, step2);
							stepperGuard.next(true);
							stepperGuard.complete()
						}

					})
				})
			}
		} else if (step == '3') {
			let step3Data = JSON.parse(localStorage.getItem('step3Data'));
			let step2Data = JSON.parse(localStorage.getItem('step2Data'))


			if (step3Data && step2Data && step2Data.mappingIsValidate) {
				return true;
			} else {
				return new Observable<boolean>((stepperGuard) => {
					this._ds.getOneImport(idImport).subscribe(importObj => {
						if (importObj.step < step) {
							stepperGuard.next(false);
							stepperGuard.complete();
						} else {
							const step2: Step2Data = {
								importId: importObj.id_import,
								id_field_mapping: importObj.id_field_mapping,
								mappingIsValidate: importObj.step >= 2
							}
							const step3: Step3Data = {
								importId: importObj.id_import,
								id_content_mapping: importObj.id_content_mapping,
							}
							this.stepService.setStepData(2, step2);
							this.stepService.setStepData(3, step3);
							stepperGuard.next(true);
							stepperGuard.complete();
						}

					})
				})
			}
		} else if (step == '4') {
			if (JSON.parse(localStorage.getItem('step4Data'))) return true;
			else {
				return new Observable<boolean>((stepperGuard) => {
					this._ds.getOneImport(idImport).subscribe(importObj => {
						if (importObj.step < step) {
							stepperGuard.next(false);
							stepperGuard.complete();
						} else {
							const step4: Step4Data = {
								importId: importObj.id_import,
							}
							this.stepService.setStepData(4, step4);
							stepperGuard.next(true);
							stepperGuard.complete();
						}

					})
				})
			};
		} else return false;
		// } else {
		// 	return false;
		// }
	}
}
