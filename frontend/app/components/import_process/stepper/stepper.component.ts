import { Component, Input } from '@angular/core';
import { ModuleConfig } from '../../../module.config';
import { Step } from '../../../models/enums.model';
import { ImportProcessService } from '../import-process.service'

@Component({
	selector: 'stepper',
	styleUrls: ['stepper.component.scss'],
	templateUrl: 'stepper.component.html'
})
export class StepperComponent {
	@Input() step;
	public Step = Step;
	public IMPORT_CONFIG = ModuleConfig;

	constructor(public importProcessService: ImportProcessService) { }
}
