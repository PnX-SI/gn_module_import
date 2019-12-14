import { Injectable } from '@angular/core';
import { Observable, BehaviorSubject } from 'rxjs';
import { FormGroup } from '@angular/forms';

export interface ICurStep {
	id: 'one' | 'two' | 'three';
	stepForm: FormGroup;
	type: 'next';
	data: any;
}

export interface Step1Data {
	importId?: number;
	datasetId?: number;
	formData?: {
		fileName?: string;
		encoding?: string;
		srid?: number;
		separator?: string;
	};
}

export interface Step2Data {
	importId?: number;
	srid?: any;
	id_field_mapping?: any;
	mappingIsValidate?: boolean;
	mappingRes?: any;
}

export interface Step3Data {
	importId?: number;
	table_name?: any;
	contentMappingInfo?: any;
	id_content_mapping?: number;
}

export interface Step4Data {
	importId?: number;
}

@Injectable()
export class StepsService {
	constructor() {}


	getStepData(step: 1 | 2 | 3 | 4): Step1Data | Step2Data | Step3Data | Step4Data {
		switch (step) {
			case 1: {
				return JSON.parse(localStorage.getItem('step1Data'));
			}
			case 2: {
				return JSON.parse(localStorage.getItem('step2Data'));
			}
			case 3: {
				return JSON.parse(localStorage.getItem('step3Data'));
			}
			case 4: {
				return JSON.parse(localStorage.getItem('step4Data'));
			}
		}
	}


	setStepData(step: 1 | 2 | 3 | 4, data?: Step1Data | Step2Data | Step3Data | Step4Data) {
		switch (step) {
			case 1: {
				localStorage.setItem('startPorcess', JSON.stringify(true));
				if (data) localStorage.setItem('step1Data', JSON.stringify(data));
				break;
			}
			case 2: {
				localStorage.setItem('step2Data', JSON.stringify(data));
				break;
			}
			case 3: {
				localStorage.setItem('step3Data', JSON.stringify(data));
				break;
			}
			case 4: {
				localStorage.setItem('step4Data', JSON.stringify(data));
				break;
			}
		}
	}


	resetStepoer() {
		localStorage.removeItem('startPorcess');
		localStorage.removeItem('step1Data');
		localStorage.removeItem('step2Data');
		localStorage.removeItem('step3Data');
		localStorage.removeItem('step4Data');
	}
}
