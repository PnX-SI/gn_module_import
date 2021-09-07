import { ValidatorFn, AbstractControl, ValidationErrors} from '@angular/forms'


export function forbiddenNameValidator(namesList: Array<string>): ValidatorFn {
	return (control: AbstractControl): ValidationErrors | null => {
	  const forbidden = namesList.includes(control.value);
	  return forbidden ? {forbiddenName: {value: control.value}} : null;
	};
      }
