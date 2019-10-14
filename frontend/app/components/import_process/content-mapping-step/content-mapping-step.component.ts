import { Component, OnInit, Input } from '@angular/core';
import { StepsService } from '../steps.service';


@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})

export class ContentMappingStepComponent implements OnInit {

	public isCollapsed = false;
	@Input() contentMappingInfo: any;

	constructor(
		private stepService: StepsService
	) {}

	ngOnInit() {
	}

	onStepBack() {
		this.stepService.previousStep();
	}
}
