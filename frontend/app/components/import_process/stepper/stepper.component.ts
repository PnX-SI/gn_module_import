import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
//import { ModuleConfig } from '../../../module.config';
import * as _ from 'lodash';

@Component({
	selector: 'stepper',
	styleUrls: [ 'stepper.component.scss' ],
	templateUrl: 'stepper.component.html'
})
export class stepperComponent implements OnInit, OnChanges {
	@Input() step: any;
	constructor() {}

	ngOnInit() {
	}

	ngOnChanges() {}
}
