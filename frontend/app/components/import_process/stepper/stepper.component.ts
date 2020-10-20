import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { Router } from '@angular/router';
import { ModuleConfig } from '../../../module.config';
import { StepsService } from '../steps.service'

@Component({
	selector: 'stepper',
	styleUrls: ['stepper.component.scss'],
	templateUrl: 'stepper.component.html'
})

export class stepperComponent implements OnInit, OnChanges {

	@Input() step: any;
	public IMPORT_CONFIG = ModuleConfig;

	constructor(public stepService: StepsService, private _router: Router) { }


	ngOnInit() { }


	onGoToStep(step) {
		if (step === 1) {
			this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/${step}`]);
		} else {
			const stepData = this.stepService.getStepData(2);
			this._router.navigate([`${ModuleConfig.MODULE_URL}/process/id_import/${stepData.importId}/step/${step}`]);
		}

	}


	ngOnChanges() { }

}
