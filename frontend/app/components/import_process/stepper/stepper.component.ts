import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { Router } from '@angular/router';
import { ModuleConfig } from '../../../module.config';

@Component({
	selector: 'stepper',
	styleUrls: [ 'stepper.component.scss' ],
	templateUrl: 'stepper.component.html'
})

export class stepperComponent implements OnInit, OnChanges {

    @Input() step: any;
    
	constructor(private _router: Router) {}


	ngOnInit() {}


	onGoToStep(step){
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process/step/${step}` ]);
	}


    ngOnChanges() {}

}
